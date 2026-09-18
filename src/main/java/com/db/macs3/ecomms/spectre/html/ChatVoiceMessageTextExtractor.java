package com.db.macs3.ecomms.spectre.html;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts just the {@code message_text} column's content out of a CHAT/VOICE
 * message's {@code content.raw_text} — an HTML table report where every row
 * is one channel event and most columns (room name, event type, timestamps,
 * attachment metadata, ...) are never scannable message content. Only the
 * {@code <td>} cell(s) whose {@code class} attribute contains the token
 * {@code message_text} (alongside a display-styling class like
 * {@code message-text-cell}, e.g. {@code class="message-text-cell message_text"})
 * ever hold real message text; every other column must never reach Hyperscan.
 *
 * <h2>Why this exists — see {@code HtmlStrippingService} class Javadoc for the underlying problem</h2>
 * <p>Simply running {@link HtmlStrippingService#strip} over the WHOLE
 * {@code raw_text} table would not just fail to isolate the message content —
 * it would feed Hyperscan every other column's text too (company names,
 * email addresses, event types, filenames), which can spuriously match a
 * lexicon term that was never about this message's real content at all.
 *
 * <h2>Two-stage stripping, composed into ONE offset map back to {@code raw_text}</h2>
 * <p>{@link #extract} pulls every matching {@code <td>}'s raw inner HTML
 * (which can itself carry tags, e.g. {@code <td class="...message_text">
 * <p>Can you share market price?</p></td>}) out of {@code raw_text},
 * concatenated with a single space between cells, together with an
 * {@link HtmlStrippingService.OffsetMap} that maps every position in that
 * concatenated text back to its real position in {@code raw_text}. The
 * caller then runs {@link HtmlStrippingService#stripExtracted} over the
 * result, which performs the SAME tag/whitespace stripping {@link HtmlStrippingService#strip}
 * always has, but composes this extraction's own offset map with the
 * stripping pass's own, so a Hyperscan match's position resolves straight
 * back to {@code raw_text} — never to this class's own intermediate,
 * cells-only text.
 *
 * <h2>Not every CHAT/VOICE message is this table shape</h2>
 * <p>{@link ExtractionResult#anyCellFound()} is false when {@code raw_text}
 * has no {@code message_text} cell at all (including when it isn't this
 * table shape in the first place) — the caller must fall back to stripping
 * {@code raw_text} directly in that case (same as the EMAIL path), rather
 * than silently treating the message as having no content.
 */
public final class ChatVoiceMessageTextExtractor {

    /**
     * The one class token that marks a cell as real message content — matched as a whitespace-
     * delimited token of the {@code class} attribute's value, not a substring (so
     * {@code "message-text-cell"} alone, without this exact token also present, does not qualify).
     */
    private static final String MESSAGE_TEXT_CLASS_TOKEN = "message_text";

    /**
     * Group 1: the opening {@code <td ...>} tag's attributes (to inspect its {@code class=}).
     * Group 2: the cell's raw inner HTML, up to (not including) its closing {@code </td>}.
     * Reluctant ({@code .*?}) so each match stops at the NEAREST {@code </td>} — real HTML
     * tables never nest a {@code <td>} inside another {@code <td>}, so this is safe and avoids
     * needing a full HTML parser for this narrowly-shaped input.
     */
    private static final Pattern TD_PATTERN = Pattern.compile(
            "<td\\b([^>]*)>(.*?)</td\\s*>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern CLASS_ATTRIBUTE_PATTERN = Pattern.compile(
            "\\bclass\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)')", Pattern.CASE_INSENSITIVE);

    private ChatVoiceMessageTextExtractor() {
    }

    /**
     * @param extractedText every matched cell's raw inner HTML, in document order, joined by a
     *                      single space — never itself scanned directly; run it through
     *                      {@link HtmlStrippingService#stripExtracted} first
     * @param offsetMap     maps a position in {@code extractedText} back to its real position in
     *                      the {@code raw_text} {@link #extract} was called with
     * @param anyCellFound  false iff {@code raw_text} had no {@code message_text} cell at all — see
     *                      class Javadoc "Not every CHAT/VOICE message is this table shape"
     */
    public record ExtractionResult(String extractedText, HtmlStrippingService.OffsetMap offsetMap,
                                   boolean anyCellFound) {
    }

    /**
     * @param rawText the CHAT/VOICE message's {@code content.raw_text} — the full HTML table document
     */
    public static ExtractionResult extract(String rawText) {
        if (rawText == null || rawText.isEmpty()) {
            return new ExtractionResult("", HtmlStrippingService.OffsetMap.of(new int[]{0}), false);
        }

        List<Cell> cells = findMessageTextCells(rawText);
        if (cells.isEmpty()) {
            return new ExtractionResult("", HtmlStrippingService.OffsetMap.of(new int[]{0}), false);
        }

        StringBuilder extracted = new StringBuilder();
        // One boundary entry per emitted character, plus a final end-of-text boundary — same
        // shape HtmlStrippingService.strip's own boundaries array uses.
        int[] boundariesBuf = new int[estimateMaxBoundaryCount(cells)];
        int extractedLen = 0;
        int previousCellEnd = -1;

        for (Cell cell : cells) {
            if (previousCellEnd >= 0) {
                // The synthetic separator between two cells has no real single character in
                // raw_text of its own — map it to the gap right after the previous cell's own
                // content, so a match spanning this separator still resolves to a real,
                // monotonically-increasing raw_text position (see HtmlStrippingService class
                // Javadoc "original-text coordinates" — a span may legitimately cover markup
                // that isn't part of matchedText itself).
                extracted.append(' ');
                boundariesBuf[extractedLen] = previousCellEnd;
                extractedLen++;
            }
            String content = cell.content();
            for (int i = 0; i < content.length(); i++) {
                extracted.append(content.charAt(i));
                boundariesBuf[extractedLen] = cell.contentStart() + i;
                extractedLen++;
            }
            previousCellEnd = cell.contentEnd();
        }
        boundariesBuf[extractedLen] = previousCellEnd;

        int[] boundaries = new int[extractedLen + 1];
        System.arraycopy(boundariesBuf, 0, boundaries, 0, extractedLen + 1);
        return new ExtractionResult(extracted.toString(), HtmlStrippingService.OffsetMap.of(boundaries), true);
    }

    /**
     * One matched {@code message_text} {@code <td>} cell's raw inner HTML and its absolute
     * position within {@code raw_text}.
     */
    private record Cell(String content, int contentStart, int contentEnd) {
    }

    private static List<Cell> findMessageTextCells(String rawText) {
        List<Cell> cells = new ArrayList<>();
        Matcher tdMatcher = TD_PATTERN.matcher(rawText);
        while (tdMatcher.find()) {
            if (hasMessageTextClass(tdMatcher.group(1))) {
                cells.add(new Cell(tdMatcher.group(2), tdMatcher.start(2), tdMatcher.end(2)));
            }
        }
        return cells;
    }

    private static boolean hasMessageTextClass(String tdAttributes) {
        Matcher classMatcher = CLASS_ATTRIBUTE_PATTERN.matcher(tdAttributes);
        if (!classMatcher.find()) {
            return false;
        }
        String classValue = classMatcher.group(1) != null ? classMatcher.group(1) : classMatcher.group(2);
        for (String token : classValue.trim().split("\\s+")) {
            if (token.equalsIgnoreCase(MESSAGE_TEXT_CLASS_TOKEN)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Upper bound on how many boundary entries {@link #extract} could ever emit: one separator
     * per cell after the first, plus every cell's own content length, plus the final end-of-text
     * boundary — sized once, up front, rather than growing a buffer per character.
     */
    private static int estimateMaxBoundaryCount(List<Cell> cells) {
        int count = 1 + (cells.size() - 1); // final boundary + one separator per cell after the first
        for (Cell cell : cells) {
            count += cell.content().length();
        }
        return count;
    }
}
