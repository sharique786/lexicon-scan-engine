package com.db.macs3.ecomms.spectre.html;

import java.io.Serial;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns message text into the normalised "clean text" Hyperscan scans — HTML tags removed, whitespace and commas collapsed
 * (line breaks kept, see below) —
 * while preserving the ability to report a match's position against the
 * ORIGINAL (un-stripped) text.
 *
 * <h2>Why this exists</h2>
 * <p>A term like {@code Enjoy(?:\s+\S+){0,2}\s+Happy} requires whitespace
 * between "Enjoy" and "Happy". A message body such as
 * {@code "<p>Enjoy</p>\n<p>Happy Birthday</p>"} would never match that
 * pattern as written, because the two words are separated by HTML markup,
 * not whitespace. This service replaces every contiguous run of HTML tags,
 * whitespace (newlines, tabs, repeated spaces, no-break spaces) and/or commas with exactly ONE character: a
 * newline ({@code \n}) if the run contains a line break ({@code \n} or {@code \r}), otherwise a
 * space. So {@code "<p>Enjoy</p>\n<p>Happy Birthday</p>"} becomes
 * {@code " Enjoy\nHappy Birthday "}, which the pattern matches correctly ({@code \s} matches the newline).
 *
 * <h2>Line breaks are kept (collapsed to one)</h2>
 * <p>Consecutive line breaks — {@code "\n\n\n"}, {@code "\r\n"}, {@code "\n  \n"}, {@code "</p>\n\n<p>"} —
 * become a single {@code \n}. This keeps the text's line structure so a disclaimer term ending in {@code .*}
 * (see {@code CollapsingScanner}) extends to the end of its own line rather than into the rest of the
 * message. Only literal line-break characters count: a tag such as {@code <br>} or {@code </p>} with no
 * newline next to it still becomes a space. Consequence: a term written with a literal space (e.g.
 * {@code mouth shut}) does not match words split across two lines; a term using {@code \s} does.
 *
 * <h2>Original-text coordinates, not stripped-text coordinates</h2>
 * <p>Hyperscan reports a match's position in terms of the STRIPPED text
 * (since that is what it scanned). {@link #strip} also returns an
 * {@link OffsetMap} that translates any stripped-text character position
 * back to where it sits in the ORIGINAL text — this is what lets a caller
 * report {@code startCharIndex}/{@code endCharIndex} against text the
 * analyst/downstream consumer actually recognises, HTML and all, rather
 * than an internal, invisible-to-them stripped form.
 *
 * <p><b>{@code startCharIndex}/{@code endCharIndex} mark a SPAN of the
 * original text, not necessarily an exact substring equal to
 * {@code matchedText}.</b> When HTML tags fall between two matched words,
 * the original-text span between {@code startCharIndex} and
 * {@code endCharIndex} contains those tags too — {@code matchedText} itself
 * is always the clean, stripped-text form of what actually matched. For
 * {@code "<p>Enjoy</p>\n<p>Happy Birthday</p>"} matched against
 * {@code Enjoy(?:\s+\S+){0,2}\s+Happy}, the result is
 * {@code {startCharIndex: 3, endCharIndex: 21, matchedText: "Enjoy\nHappy"}} —
 * {@code original.substring(3, 21)} is {@code "Enjoy</p>\n<p>Happy"}, which
 * spans the same real-world content as {@code matchedText} once its HTML is
 * mentally stripped back out, not a literal character-for-character match.
 *
 * <h2>When stripping applies</h2>
 * <p>{@link #strip} is applied to the subject and to the message body (EMAIL {@code msg_text} directly; CHAT/VOICE
 * {@code msg_text} through {@code ChatVoiceMessageTextExtractor} first — see {@link #stripExtracted}). It is idempotent on
 * text that has no tags and only single spaces (identity mapping). Attachment {@code attachment_text} is never stripped: it is
 * already free of markup, so {@link #identity} skips the scan and the offset array.
 */
public final class HtmlStrippingService {

    /**
     * Matches one HTML tag: {@code <}, anything but {@code >}, {@code >}.
     */
    private static final Pattern TAG_PATTERN = Pattern.compile("<[^>]*>");

    private HtmlStrippingService() {
    }

    public static final class StripResult implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private final String strippedText;
        private final OffsetMap offsetMap;

        /**
         * Result of {@link #strip}.
         *
         * @param strippedText the text with every HTML-tag/whitespace/comma run
         *                     collapsed to exactly one space (one newline if the
         *                     run contained a line break) — what Hyperscan
         *                     actually scans
         * @param offsetMap    translates a stripped-text position back to its
         *                     original-text position — see {@link OffsetMap}
         */
        public StripResult(String strippedText, OffsetMap offsetMap) {
            this.strippedText = strippedText;
            this.offsetMap = offsetMap;
        }

        public String strippedText() {
            return strippedText;
        }

        public OffsetMap offsetMap() {
            return offsetMap;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            StripResult other = (StripResult) obj;
            return java.util.Objects.equals(strippedText, other.strippedText)
                    && java.util.Objects.equals(offsetMap, other.offsetMap);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(strippedText, offsetMap);
        }

        @Override
        public String toString() {
            return "StripResult[strippedText=" + strippedText + ", offsetMap=" + offsetMap + "]";
        }
    }

    /**
     * Maps a position in stripped text back to the corresponding position in
     * the original (pre-stripping) text.
     *
     * <p>Built as a {@code strippedLength + 1}-entry array — one entry per
     * possible stripped-text BOUNDARY (before each character, plus one for
     * the end of the string) rather than per character, so that both a
     * match's start (inclusive) and end (exclusive) position can be mapped
     * with the same lookup and no special-casing at the end of the string.
     *
     * <p><b>Except for {@link #identity()}</b> — a null {@code boundaries}
     * array means "stripped position == original position," computed in O(1)
     * with no backing array at all. This exists specifically for content
     * that {@link #strip} would leave byte-for-byte unchanged anyway (e.g.
     * attachment {@code attachmentText}, already HTML-free by the time it reaches
     * this engine — see {@code MessageAttachment} class Javadoc): running the
     * full tag/whitespace-scanning algorithm AND allocating an {@code int[]}
     * sized to the text's length is pure waste on text that can be
     * megabytes long, for a transform guaranteed to be a no-op.
     */
    public static final class OffsetMap implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private static final OffsetMap IDENTITY = new OffsetMap(null);

        private final int[] boundaries;

        private OffsetMap(int[] boundaries) {
            this.boundaries = boundaries;
        }

        /**
         * @return an offset map where every position maps to itself, built in O(1) — see class Javadoc.
         */
        public static OffsetMap identity() {
            return IDENTITY;
        }

        /**
         * Builds an offset map directly from a caller-supplied boundaries array — for a caller
         * that computes its own stripped-position → original-position mapping outside this
         * class's own {@link #strip} algorithm (see {@code ChatVoiceMessageTextExtractor}, which
         * maps an extracted subset of a document's text back to that document's own positions,
         * not the {@code strip}-algorithm's tag/whitespace-run boundaries this class normally
         * produces). {@code boundaries} must have one entry per stripped-text boundary position,
         * {@code 0..strippedLength} inclusive (length {@code strippedLength + 1}), exactly the
         * same contract {@link #strip} itself builds — see that method's own boundaries array
         * for a worked example.
         */
        public static OffsetMap of(int[] boundaries) {
            if (boundaries == null || boundaries.length == 0) {
                throw new IllegalArgumentException("boundaries must be non-null and non-empty");
            }
            return new OffsetMap(boundaries);
        }

        /**
         * @param strippedPosition a boundary position in the stripped text, {@code 0..strippedLength} inclusive
         * @return the corresponding boundary position in the original text
         */
        public int toOriginal(int strippedPosition) {
            if (boundaries == null) {
                if (strippedPosition < 0) {
                    throw new IndexOutOfBoundsException("strippedPosition " + strippedPosition + " must be >= 0");
                }
                return strippedPosition;
            }
            if (strippedPosition < 0 || strippedPosition >= boundaries.length) {
                throw new IndexOutOfBoundsException(
                        "strippedPosition " + strippedPosition + " out of range [0, " + (boundaries.length - 1) + "]");
            }
            return boundaries[strippedPosition];
        }
    }

    /**
     * A {@link StripResult} for text KNOWN to need no stripping at all — see
     * {@link OffsetMap#identity()}. Skips the whole tag/whitespace-scanning
     * pass and its {@code int[]} allocation entirely, unlike calling
     * {@link #strip} on already-clean text (which would produce an
     * equivalent result, just via O(n) work and O(n) memory it doesn't need
     * to spend). {@code null} input yields an empty result, matching
     * {@link #strip}'s own null handling.
     */
    public static StripResult identity(String text) {
        return new StripResult(text == null ? "" : text, OffsetMap.identity());
    }

    /**
     * {@link #strip}, but for text that is itself already an EXTRACT of some larger original
     * document — {@code extractedText} is not the original document, and {@code extractionOffsetMap}
     * already maps every one of {@code extractedText}'s own positions back to that larger
     * document's real positions (see {@code ChatVoiceMessageTextExtractor}, whose
     * {@code <td>}-cell extraction pass this exists for). Runs the normal tag/whitespace-stripping
     * pass on {@code extractedText} exactly as {@link #strip} would, then COMPOSES the two offset
     * maps — {@code extractedText} position → {@code extractionOffsetMap} → original-document
     * position — so the returned {@link StripResult#offsetMap()} maps a Hyperscan match's position
     * (in the doubly-stripped text this method returns) straight back to the ORIGINAL document's
     * positions, never to the intermediate {@code extractedText}'s own positions.
     */
    public static StripResult stripExtracted(String extractedText, OffsetMap extractionOffsetMap) {
        StripResult innerStrip = strip(extractedText);
        int boundaryCount = innerStrip.strippedText().length() + 1;
        int[] composedBoundaries = new int[boundaryCount];
        for (int i = 0; i < boundaryCount; i++) {
            composedBoundaries[i] = extractionOffsetMap.toOriginal(innerStrip.offsetMap().toOriginal(i));
        }
        return new StripResult(innerStrip.strippedText(), new OffsetMap(composedBoundaries));
    }

    /**
     * Strips HTML from {@code originalText} and builds the offset map back
     * to it. Never returns null; an empty/null input yields an empty result.
     */
    public static StripResult strip(String originalText) {
        if (originalText == null || originalText.isEmpty()) {
            return new StripResult("", new OffsetMap(new int[]{0}));
        }

        StringBuilder stripped = new StringBuilder(originalText.length());
        // One boundary entry per emitted stripped character, plus a final entry
        // for the end-of-string boundary — appended after the loop.
        int[] boundariesBuf = new int[originalText.length() + 1];
        boolean[] runHasLineBreak = new boolean[1]; // out-parameter of consumeTagOrWhitespaceRun
        int strippedLen = 0;

        int originalIndex = 0;
        int textLength = originalText.length();
        Matcher tagMatcher = TAG_PATTERN.matcher(originalText);

        while (originalIndex < textLength) {
            char currentChar = originalText.charAt(originalIndex);
            boolean isTagStart = currentChar == '<' && tagMatcher.region(originalIndex, textLength).lookingAt();

            if (isTagStart || isSeparator(currentChar)) {
                // Consume this whole contiguous run of tags, whitespace and/or commas as ONE unit:
                // a single newline if the run holds a line break, else a single space.
                int runStart = originalIndex;
                int scanIndex = consumeTagOrWhitespaceRun(originalText, originalIndex, textLength, tagMatcher,
                        runHasLineBreak);
                stripped.append(runHasLineBreak[0] ? '\n' : ' ');
                boundariesBuf[strippedLen] = runStart;
                strippedLen++;
                originalIndex = scanIndex;
            } else {
                stripped.append(currentChar);
                boundariesBuf[strippedLen] = originalIndex;
                strippedLen++;
                originalIndex++;
            }
        }
        // Final boundary: the end of the original text (one past its last character).
        boundariesBuf[strippedLen] = textLength;

        int[] boundaries = new int[strippedLen + 1];
        System.arraycopy(boundariesBuf, 0, boundaries, 0, strippedLen + 1);

        return new StripResult(stripped.toString(), new OffsetMap(boundaries));
    }

    /**
     * True for a character that is replaced by (and merged into) a single space (or a single newline, see
     * {@link #isLineBreak}): any whitespace (space, tab, newline, carriage return, form feed, ...), any Unicode
     * space separator — including the no-break spaces {@code U+00A0}, {@code U+2007}, {@code U+202F} that
     * {@link Character#isWhitespace} excludes but HTML email bodies use heavily — or a comma: ASCII {@code ,},
     * fullwidth {@code U+FF0C} (CJK text) or Arabic {@code U+060C}.
     */
    static boolean isSeparator(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == ',' || c == '\uFF0C' || c == '\u060C';
    }

    /** True for a line-break character; a separator run containing one collapses to {@code \n}, not a space. */
    static boolean isLineBreak(char c) {
        return c == '\n' || c == '\r';
    }

    /**
     * @param hasLineBreak out-parameter: {@code hasLineBreak[0]} is set to whether the run contains a line-break
     *                     character outside a tag (a newline inside a tag's attributes does not count)
     * @return the index one past the end of the contiguous run of HTML tags, whitespace and/or commas
     *         starting at {@code start}.
     */
    private static int consumeTagOrWhitespaceRun(String originalText, int start, int textLength, Matcher tagMatcher,
                                                 boolean[] hasLineBreak) {
        hasLineBreak[0] = false;
        int scanIndex = start;
        while (scanIndex < textLength) {
            char scanChar = originalText.charAt(scanIndex);
            if (scanChar == '<' && tagMatcher.region(scanIndex, textLength).lookingAt()) {
                scanIndex = tagMatcher.end();
            } else if (isSeparator(scanChar)) {
                if (isLineBreak(scanChar)) {
                    hasLineBreak[0] = true;
                }
                scanIndex++;
            } else {
                break;
            }
        }
        return scanIndex;
    }
}
