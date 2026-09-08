package com.db.macs3.ecomms.spectre.model.match;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Every match found for ONE raw Hyperscan expression id within a single
 * area of a message — NOT yet resolved to a term identity or evaluated for
 * an AND NOT boolean condition. {@link com.db.macs3.ecomms.spectre.hyperscan.HyperscanScanService#scan}
 * returns these; {@code FeatureScanOrchestrator} merges them across every
 * area a feature's scope covers, then resolves and evaluates them into
 * final {@link TermMatchResult}s using {@code TermExpressionMetadata}.
 */
public class RawExpressionMatch implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private int expressionId;
    private String matchedPatternText;
    private List<AreaMatch> matches;

    /**
     * @param expressionId       the raw Hyperscan expression id, as reported on a {@code Match} —
     *                           may belong to a simple/decomposed term's own reportable id, or to
     *                           ONE of an AND NOT term's required/excluded ids; the caller
     *                           (with {@code TermExpressionMetadata}) determines which
     * @param matchedPatternText the pattern text read directly off the {@code Match}'s own
     *                           {@code Expression} — reliable for a plain or COMBINATION
     *                           expression's own text; not meaningful as a display value for an
     *                           AND NOT term's excluded-side id (see {@code FeatureScanOrchestrator})
     * @param matches            every occurrence found for this expression id within this one area —
     *                           non-empty
     */
    public RawExpressionMatch(int expressionId, String matchedPatternText, List<AreaMatch> matches) {
        if (matches == null || matches.isEmpty()) {
            throw new IllegalArgumentException("RawExpressionMatch requires at least one match for expressionId=" + expressionId);
        }
        this.expressionId = expressionId;
        this.matchedPatternText = matchedPatternText;
        this.matches = matches;
    }

    public int getExpressionId() {
        return expressionId;
    }

    public void setExpressionId(int expressionId) {
        this.expressionId = expressionId;
    }

    public String getMatchedPatternText() {
        return matchedPatternText;
    }

    public void setMatchedPatternText(String matchedPatternText) {
        this.matchedPatternText = matchedPatternText;
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
        if (!(obj instanceof RawExpressionMatch)) {
            return false;
        }
        RawExpressionMatch other = (RawExpressionMatch) obj;
        return expressionId == other.expressionId
                && Objects.equals(matchedPatternText, other.matchedPatternText)
                && Objects.equals(matches, other.matches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressionId, matchedPatternText, matches);
    }

    @Override
    public String toString() {
        return "RawExpressionMatch[expressionId=" + expressionId + ", matchedPatternText=" + matchedPatternText
                + ", matches=" + matches + "]";
    }
}
