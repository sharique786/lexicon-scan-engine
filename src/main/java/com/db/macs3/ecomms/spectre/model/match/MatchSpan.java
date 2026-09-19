package com.db.macs3.ecomms.spectre.model.match;

import java.io.Serial;
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
public class MatchSpan implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private int startCharIndex;
    private int endCharIndex;
    private String matchedText;

    /**
     * @param startCharIndex inclusive start offset in the ORIGINAL text
     * @param endCharIndex   exclusive end offset in the ORIGINAL text
     * @param matchedText    the substring of the ORIGINAL text this match covers —
     *                       {@code originalText.substring(startCharIndex, endCharIndex)}
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

    public int getStartCharIndex() {
        return startCharIndex;
    }

    public void setStartCharIndex(int startCharIndex) {
        this.startCharIndex = startCharIndex;
    }

    public int getEndCharIndex() {
        return endCharIndex;
    }

    public void setEndCharIndex(int endCharIndex) {
        this.endCharIndex = endCharIndex;
    }

    public String getMatchedText() {
        return matchedText;
    }

    public void setMatchedText(String matchedText) {
        this.matchedText = matchedText;
    }

    public int length() {
        return endCharIndex - startCharIndex;
    }

    /**
     * @return true iff this span is entirely inside {@code other} — the
     * "full containment" rule used for disclaimer-precedence
     * suppression (see {@code DecisionTreeEvaluator}); a span that
     * merely overlaps {@code other} without being fully inside it
     * returns false.
     */
    public boolean isFullyContainedIn(MatchSpan other) {
        return this.startCharIndex >= other.startCharIndex && this.endCharIndex <= other.endCharIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        MatchSpan other = (MatchSpan) obj;
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
