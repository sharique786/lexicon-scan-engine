package com.db.macs3.ecomms.spectre.model.match;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Every match found for ONE lexicon term across a single message — potentially spanning the subject, the message
 * body and several attachments, and more than one occurrence within any of them. A term is one entry of a feature's
 * compile-results JSON; it can be backed by one or several Hyperscan expressions, or verified in Java (see
 * {@code FeatureScanOrchestrator}).
 */
public class TermMatchResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String termId;
    private String termDescription;
    private String termRegexPattern;
    private List<AreaMatch> matches;

    /**
     * @param termId           {@code <body.lexiconName>::<termNumber>} — see
     *                         {@code TermIdBuilder}
     * @param termRegexPattern the term's readable pattern text (its {@code resolvedPatterns}, else its regex(es) joined),
     *                         for {@code lexicon-hit-summary.term_dtls.term_regex_pattern}
     * @param matches          every occurrence found, tagged by area — non-empty
     *                         (a term with zero matches is simply absent from a
     *                         feature's result, not represented by an empty-matches instance)
     */
    public TermMatchResult(String termId, String termRegexPattern, List<AreaMatch> matches) {
        this(termId, termRegexPattern, null, matches);
    }

    /**
     * @param termId           {@code <body.lexiconName>::<termNumber>} — see
     *                         {@code TermIdBuilder}
     * @param termRegexPattern the term's readable pattern text (its {@code resolvedPatterns}, else its regex(es) joined),
     *                         for {@code lexicon-hit-summary.term_dtls.term_regex_pattern}
     * @param termDescription  the Compile Service's own {@code termDescription} — the original,
     *                         analyst-authored lexicon term text, verbatim, for
     *                         {@code lexicon-hit-summary.term_dtls.term_description}. May be null
     *                         (e.g. a caller with no {@code TermExpressionMetadata} to source it from).
     * @param matches          every occurrence found, tagged by area — non-empty
     *                         (a term with zero matches is simply absent from a
     *                         feature's result, not represented by an empty-matches instance)
     */
    public TermMatchResult(String termId, String termRegexPattern, String termDescription, List<AreaMatch> matches) {
        if (matches == null || matches.isEmpty()) {
            throw new IllegalArgumentException("TermMatchResult requires at least one match for termId=" + termId);
        }
        this.termId = termId;
        this.termRegexPattern = termRegexPattern;
        this.termDescription = termDescription;
        this.matches = matches;
    }

    public String getTermId() {
        return termId;
    }

    public void setTermId(String termId) {
        this.termId = termId;
    }

    public String getTermDescription() {
        return termDescription;
    }

    public void setTermDescription(String termDescription) {
        this.termDescription = termDescription;
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
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        TermMatchResult other = (TermMatchResult) obj;
        return Objects.equals(termId, other.termId)
                && Objects.equals(termRegexPattern, other.termRegexPattern)
                && Objects.equals(termDescription, other.termDescription)
                && Objects.equals(matches, other.matches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(termId, termRegexPattern, termDescription, matches);
    }

    @Override
    public String toString() {
        return "TermMatchResult[termId=" + termId + ", termRegexPattern=" + termRegexPattern
                + ", termDescription=" + termDescription + ", matches=" + matches + "]";
    }
}
