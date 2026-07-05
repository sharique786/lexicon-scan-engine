package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single match produced by the Hyperscan scanner against a text
 * segment (message body or one attachment), enriched with the human-readable
 * term identity resolved from the feature's {@link TermManifestEntry manifest}.
 *
 * <p>Positions are byte offsets within the scanned UTF-8 text. {@link #delta}
 * is the gap between the end of the previous match and the start of this one,
 * within the same text segment (zero for the first match).
 */
public class TermMatch implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Hyperscan's numeric expression id — 0-based position in the combined .hdb. */
    private int expressionId;

    /** Human-readable term id resolved via the manifest, e.g. "lexicon_market_cond_2::1". */
    private String termId;

    /** The original compiled PCRE pattern for this term, resolved via the manifest. */
    private String termRegexPattern;

    /** The substring of the input text that matched. */
    private String matchText;

    private long startIndex;
    private long endIndex;
    private long delta;

    public TermMatch() {}

    public static TermMatch of(int expressionId, String termId, String termRegexPattern,
                                String matchText, long startIndex, long endIndex, long delta) {
        TermMatch m = new TermMatch();
        m.expressionId     = expressionId;
        m.termId            = termId;
        m.termRegexPattern  = termRegexPattern;
        m.matchText         = matchText;
        m.startIndex        = startIndex;
        m.endIndex          = endIndex;
        m.delta             = delta;
        return m;
    }

    public int getExpressionId()             { return expressionId; }
    public void setExpressionId(int v)       { this.expressionId = v; }
    public String getTermId()                { return termId; }
    public void setTermId(String v)          { this.termId = v; }
    public String getTermRegexPattern()      { return termRegexPattern; }
    public void setTermRegexPattern(String v){ this.termRegexPattern = v; }
    public String getMatchText()             { return matchText; }
    public void setMatchText(String v)       { this.matchText = v; }
    public long getStartIndex()              { return startIndex; }
    public void setStartIndex(long v)        { this.startIndex = v; }
    public long getEndIndex()                { return endIndex; }
    public void setEndIndex(long v)          { this.endIndex = v; }
    public long getDelta()                   { return delta; }
    public void setDelta(long v)             { this.delta = v; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermMatch that = (TermMatch) o;
        return expressionId == that.expressionId && startIndex == that.startIndex && endIndex == that.endIndex;
    }

    @Override
    public int hashCode() { return Objects.hash(expressionId, startIndex, endIndex); }

    @Override
    public String toString() {
        return "TermMatch{termId='" + termId + "', text='" + matchText +
               "', start=" + startIndex + ", end=" + endIndex + ", delta=" + delta + '}';
    }
}
