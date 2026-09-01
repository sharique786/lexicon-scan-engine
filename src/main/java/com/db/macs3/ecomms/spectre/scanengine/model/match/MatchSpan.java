package com.db.macs3.ecomms.spectre.scanengine.model.match;

import java.io.Serializable;
import java.util.Objects;

/**
 * One Hyperscan match, reported at ORIGINAL-text character coordinates —
 * never at HTML-stripped-text coordinates. See
 * {@code HtmlStrippingService} for the offset map that makes this possible:
 * Hyperscan scans the stripped text, but every match reported here has
 * already been translated back to where that text actually sits in the
 * message as it was originally written.
 */
public final class MatchSpan implements Serializable {

    private final int startCharIndex;
    private final int endCharIndex;
    private final String matchedText;

    /**
     * @param startCharIndex    inclusive start offset in the ORIGINAL text
     * @param endCharIndex       exclusive end offset in the ORIGINAL text
     * @param matchedText         the substring of the ORIGINAL text this match covers —
     *                             {@code originalText.substring(startCharIndex, endCharIndex)}
     */
    public MatchSpan(int startCharIndex, int endCharIndex, String matchedText) {
        if (startCharIndex < 0 || endCharIndex < startCharIndex) {
            throw new IllegalArgumentException(
                    "Invalid match span: startCharIndex=" + startCharIndex + ", endCharIndex=" + endCharIndex);
        }
        this.startCharIndex = startCharIndex;
        this.endCharIndex = endCharIndex;
        this.matchedText = matchedText;
    }

    public int startCharIndex() { return startCharIndex; }
    public int endCharIndex() { return endCharIndex; }
    public String matchedText() { return matchedText; }

    public int length() {
        return endCharIndex - startCharIndex;
    }

    /**
     * @return true iff this span is entirely inside {@code other} — the
     *         "full containment" rule used for disclaimer-precedence
     *         suppression (see {@code DecisionTreeEvaluator}); a span that
     *         merely overlaps {@code other} without being fully inside it
     *         returns false.
     */
    public boolean isFullyContainedIn(MatchSpan other) {
        return this.startCharIndex >= other.startCharIndex && this.endCharIndex <= other.endCharIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MatchSpan)) {
            return false;
        }
        MatchSpan other = (MatchSpan) o;
        return startCharIndex == other.startCharIndex
                && endCharIndex == other.endCharIndex
                && Objects.equals(matchedText, other.matchedText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startCharIndex, endCharIndex, matchedText);
    }

    @Override
    public String toString() {
        return "MatchSpan[startCharIndex=" + startCharIndex + ", endCharIndex=" + endCharIndex
                + ", matchedText=" + matchedText + "]";
    }
}
