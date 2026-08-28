package com.db.macs3.ecomms.spectre.scanengine.html;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Strips HTML markup from message text before it is scanned by Hyperscan,
 * while preserving the ability to report a match's position against the
 * ORIGINAL (un-stripped) text — ported from the same approach used in the
 * Lexicon Scanner Service (requirement 8.a: reuse only the HTML-stripping /
 * original-text offset mapping from that service, not its disclaimer
 * detection, which the Scan Engine handles differently — see
 * {@code DecisionTreeEvaluator}).
 *
 * <h2>Why this exists</h2>
 * <p>A term like {@code Enjoy(?:\s+\S+){0,2}\s+Happy} requires whitespace
 * between "Enjoy" and "Happy". A message body such as
 * {@code "<p>Enjoy</p>\n<p>Happy Birthday</p>"} would never match that
 * pattern as written, because the two words are separated by HTML markup,
 * not whitespace. This service replaces every contiguous run of HTML tags
 * and/or literal whitespace with exactly ONE space, so
 * {@code "<p>Enjoy</p>\n<p>Happy Birthday</p>"} becomes
 * {@code " Enjoy Happy Birthday "}, which the pattern matches correctly.
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
 * {@code {startCharIndex: 3, endCharIndex: 21, matchedText: "Enjoy Happy"}} —
 * {@code original.substring(3, 21)} is {@code "Enjoy</p>\n<p>Happy"}, which
 * spans the same real-world content as {@code matchedText} once its HTML is
 * mentally stripped back out, not a literal character-for-character match.
 *
 * <h2>When stripping applies</h2>
 * <p>Applied unconditionally to every message body before scanning —
 * {@link #strip} is cheap and idempotent on HTML-free text (no tags/no
 * multi-character whitespace runs means the "stripped" text is
 * character-for-character identical to the original, and the offset map is
 * simply the identity mapping), so there is no need for a separate
 * HTML-detection pre-check per requirement 1.i.1.
 */
public final class HtmlStrippingService {

    /** Matches one HTML tag: {@code <}, anything but {@code >}, {@code >}. */
    private static final Pattern TAG_PATTERN = Pattern.compile("<[^>]*>");

    private HtmlStrippingService() {}

    /**
     * Result of {@link #strip}.
     *
     * @param strippedText    the text with every HTML-tag-or-whitespace run
     *                         collapsed to exactly one space — what Hyperscan
     *                         actually scans
     * @param offsetMap        translates a stripped-text position back to its
     *                         original-text position — see {@link OffsetMap}
     */
    public static final class StripResult implements Serializable {
        private final String strippedText;
        private final OffsetMap offsetMap;

        public StripResult(String strippedText, OffsetMap offsetMap) {
            this.strippedText = strippedText;
            this.offsetMap = offsetMap;
        }

        public String strippedText() { return strippedText; }
        public OffsetMap offsetMap() { return offsetMap; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StripResult)) return false;
            StripResult other = (StripResult) o;
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
     */
    public static final class OffsetMap implements Serializable {
        private final int[] boundaries;

        private OffsetMap(int[] boundaries) {
            this.boundaries = boundaries;
        }

        /**
         * @param strippedPosition a boundary position in the stripped text, {@code 0..strippedLength} inclusive
         * @return the corresponding boundary position in the original text
         */
        public int toOriginal(int strippedPosition) {
            if (strippedPosition < 0 || strippedPosition >= boundaries.length) {
                throw new IndexOutOfBoundsException(
                        "strippedPosition " + strippedPosition + " out of range [0, " + (boundaries.length - 1) + "]");
            }
            return boundaries[strippedPosition];
        }
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
        int strippedLen = 0;

        int i = 0;
        int n = originalText.length();
        Matcher tagMatcher = TAG_PATTERN.matcher(originalText);

        while (i < n) {
            char c = originalText.charAt(i);
            boolean isTagStart = c == '<' && tagMatcher.region(i, n).lookingAt();

            if (isTagStart || Character.isWhitespace(c)) {
                // Consume this whole contiguous run of tags and/or whitespace as ONE unit.
                int runStart = i;
                int j = i;
                while (j < n) {
                    char cj = originalText.charAt(j);
                    if (cj == '<' && tagMatcher.region(j, n).lookingAt()) {
                        j = tagMatcher.end();
                    } else if (Character.isWhitespace(cj)) {
                        j++;
                    } else {
                        break;
                    }
                }
                stripped.append(' ');
                boundariesBuf[strippedLen] = runStart;
                strippedLen++;
                i = j;
            } else {
                stripped.append(c);
                boundariesBuf[strippedLen] = i;
                strippedLen++;
                i++;
            }
        }
        // Final boundary: the end of the original text (one past its last character).
        boundariesBuf[strippedLen] = n;

        int[] boundaries = new int[strippedLen + 1];
        System.arraycopy(boundariesBuf, 0, boundaries, 0, strippedLen + 1);

        return new StripResult(stripped.toString(), new OffsetMap(boundaries));
    }
}
