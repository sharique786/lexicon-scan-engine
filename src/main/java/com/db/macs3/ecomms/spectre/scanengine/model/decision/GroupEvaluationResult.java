package com.db.macs3.ecomms.spectre.scanengine.model.decision;

import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The evaluated outcome of one {@link FeatureGroup} for one message —
 * whether the group counts as a "hit" (after resolving its
 * {@link FeatureGroup#operator()} across every member), and the raw scan
 * results behind that decision.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class GroupEvaluationResult implements Serializable {

    private final FeatureGroup group;
    private final Map<FeatureDecisionRow, List<TermMatchResult>> memberMatches;
    private final Map<FeatureDecisionRow, Boolean> memberHit;
    private final boolean isHit;

    /**
     * @param group                     the group this result is for
     * @param memberMatches              every match found for each member row —
     *                                    BEFORE disclaimer-overlap suppression (that
     *                                    suppression is applied only to Lexicon-category
     *                                    groups' matches, downstream in
     *                                    {@code MessageEvaluationResult}); a member with no
     *                                    matches is present with an empty list, not absent
     * @param memberHit                  per-member hit status — true iff that member's
     *                                    {@code featuresToApply} lexicon had at least one
     *                                    match anywhere in scope (see requirement 3 answer:
     *                                    minimumHits is informational only for this engine —
     *                                    any match at all counts as a hit)
     * @param isHit                       the group's OVERALL hit status: for a single-member
     *                                    group, equal to that member's hit; for a multi-member
     *                                    group, {@link FeatureGroup#operator()} applied across
     *                                    every {@link #memberHit} value (OR = any true, AND =
     *                                    all true — requirement 4's confirmed semantics)
     */
    public GroupEvaluationResult(FeatureGroup group, Map<FeatureDecisionRow, List<TermMatchResult>> memberMatches,
                                  Map<FeatureDecisionRow, Boolean> memberHit, boolean isHit) {
        this.group = group;
        this.memberMatches = memberMatches;
        this.memberHit = memberHit;
        this.isHit = isHit;
    }

    public FeatureGroup group() { return group; }
    public Map<FeatureDecisionRow, List<TermMatchResult>> memberMatches() { return memberMatches; }
    public Map<FeatureDecisionRow, Boolean> memberHit() { return memberHit; }
    public boolean isHit() { return isHit; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupEvaluationResult)) return false;
        GroupEvaluationResult other = (GroupEvaluationResult) o;
        return isHit == other.isHit
                && Objects.equals(group, other.group)
                && Objects.equals(memberMatches, other.memberMatches)
                && Objects.equals(memberHit, other.memberHit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, memberMatches, memberHit, isHit);
    }

    @Override
    public String toString() {
        return "GroupEvaluationResult[group=" + group + ", memberMatches=" + memberMatches
                + ", memberHit=" + memberHit + ", isHit=" + isHit + "]";
    }
}
