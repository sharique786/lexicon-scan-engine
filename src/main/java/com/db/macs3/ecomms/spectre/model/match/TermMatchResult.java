package com.db.macs3.ecomms.spectre.model.match;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Every match found for ONE term (one Hyperscan expression within one
 * feature's {@code .hdb} database) across a single message — potentially
 * spanning subject, message body, and/or multiple attachments, and
 * potentially more than one occurrence within any of those.
 */
public class TermMatchResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String termId;
    private String termRegexPattern;
    private List<AreaMatch> matches;

    /**
     * @param termId           {@code <body.lexiconName>::<index>} — see
     *                         {@code TermIdBuilder}
     * @param termRegexPattern the compiled Hyperscan pattern text for this term,
     *                         for {@code lexicon-hit-summary.term_dtls.term_regex_pattern}
     * @param matches          every occurrence found, tagged by area — non-empty
     *                         (a term with zero matches is simply absent from a
     *                         feature's result, not represented by an empty-matches instance)
     */
    public TermMatchResult(String termId, String termRegexPattern, List<AreaMatch> matches) {
        if (matches == null || matches.isEmpty()) {
            throw new IllegalArgumentException("TermMatchResult requires at least one match for termId=" + termId);
        }
        this.termId = termId;
        this.termRegexPattern = termRegexPattern;
        this.matches = matches;
    }

    public String getTermId() {
        return termId;
    }

    public void setTermId(String termId) {
        this.termId = termId;
    }

    public String getTermRegexPattern() {
        return termRegexPattern;
    }

    public void setTermRegexPattern(String termRegexPattern) {
        this.termRegexPattern = termRegexPattern;
    }

    public List<AreaMatch> getMatches() {
        return matches;
    }

    public void setMatches(List<AreaMatch> matches) {
        this.matches = matches;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TermMatchResult)) {
            return false;
        }
        TermMatchResult other = (TermMatchResult) obj;
        return Objects.equals(termId, other.termId)
                && Objects.equals(termRegexPattern, other.termRegexPattern)
                && Objects.equals(matches, other.matches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(termId, termRegexPattern, matches);
    }

    @Override
    public String toString() {
        return "TermMatchResult[termId=" + termId + ", termRegexPattern=" + termRegexPattern
                + ", matches=" + matches + "]";
    }
}
