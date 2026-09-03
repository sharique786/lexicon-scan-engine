package com.db.macs3.ecomms.spectre.scanengine.model.decision;

import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The complete decision-tree outcome for one message — every group that was
 * evaluated, in processing order, plus the final (disclaimer-suppressed)
 * Lexicon-category matches this message's output rows are built from.
 *
 * <p>See {@code DecisionTreeEvaluator} for how this is produced: groups are
 * evaluated in {@code FeatureGroupingService}'s order (NoiseReduction →
 * Disclaimer → Lexicon); if any NoiseReduction group is a hit, evaluation
 * stops there and {@link #shortCircuited} is true — every LATER group
 * (further NoiseReduction groups, the Disclaimer group, all Lexicon groups)
 * is simply never evaluated at all, not evaluated-and-discarded.
 */
public final class MessageEvaluationResult implements Serializable {

    private final String messageId;
    private final List<GroupEvaluationResult> evaluatedGroups;
    private final boolean shortCircuited;
    private final List<TermMatchResult> disclaimerMatches;
    private final Map<String, List<TermMatchResult>> finalLexiconMatchesByFeatureId;
    private final int suppressedLexiconMatchCount;

    /**
     * @param messageId                        the message this result is for
     * @param evaluatedGroups                   every group that WAS evaluated, in processing
     *                                           order — stops early (per {@link #shortCircuited})
     * @param shortCircuited                     true iff a NoiseReduction group was a hit,
     *                                           meaning Disclaimer and Lexicon groups were
     *                                           never evaluated at all
     * @param disclaimerMatches                   every disclaimer match found (empty if there was
     *                                           no disclaimer group, or it had no matches) — used to
     *                                           suppress overlapping Lexicon matches; disclaimer
     *                                           matches themselves are never suppressed
     * @param finalLexiconMatchesByFeatureId      Lexicon-category matches AFTER disclaimer-overlap
     *                                           suppression (full containment only — see
     *                                           {@code DecisionTreeEvaluator}), keyed by
     *                                           {@code featureId} (i.e. by {@link FeatureGroup#featureId()})
     *                                           so a downstream row builder can reconstruct which
     *                                           evaluated group each surviving match belongs to — this
     *                                           is what {@code lexicon-hit-summary}/{@code -restricted}/
     *                                           {@code -unrestricted} are built from; empty when
     *                                           {@link #shortCircuited} is true. A {@code featureId} with
     *                                           zero surviving matches after suppression is absent from
     *                                           this map entirely, not present with an empty list.
     * @param suppressedLexiconMatchCount        how many raw Lexicon matches were discarded by
     *                                           disclaimer-overlap suppression, for observability/audit —
     *                                           not itself written to any output table
     */
    public MessageEvaluationResult(String messageId, List<GroupEvaluationResult> evaluatedGroups,
                                    boolean shortCircuited, List<TermMatchResult> disclaimerMatches,
                                    Map<String, List<TermMatchResult>> finalLexiconMatchesByFeatureId,
                                    int suppressedLexiconMatchCount) {
        this.messageId = messageId;
        this.evaluatedGroups = evaluatedGroups;
        this.shortCircuited = shortCircuited;
        this.disclaimerMatches = disclaimerMatches;
        this.finalLexiconMatchesByFeatureId = finalLexiconMatchesByFeatureId;
        this.suppressedLexiconMatchCount = suppressedLexiconMatchCount;
    }

    public String messageId() { return messageId; }
    public List<GroupEvaluationResult> evaluatedGroups() { return evaluatedGroups; }
    public boolean shortCircuited() { return shortCircuited; }
    public List<TermMatchResult> disclaimerMatches() { return disclaimerMatches; }
    public Map<String, List<TermMatchResult>> finalLexiconMatchesByFeatureId() { return finalLexiconMatchesByFeatureId; }
    public int suppressedLexiconMatchCount() { return suppressedLexiconMatchCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageEvaluationResult)) {
            return false;
        }
        MessageEvaluationResult other = (MessageEvaluationResult) o;
        return shortCircuited == other.shortCircuited
                && suppressedLexiconMatchCount == other.suppressedLexiconMatchCount
                && Objects.equals(messageId, other.messageId)
                && Objects.equals(evaluatedGroups, other.evaluatedGroups)
                && Objects.equals(disclaimerMatches, other.disclaimerMatches)
                && Objects.equals(finalLexiconMatchesByFeatureId, other.finalLexiconMatchesByFeatureId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, evaluatedGroups, shortCircuited, disclaimerMatches,
                finalLexiconMatchesByFeatureId, suppressedLexiconMatchCount);
    }

    @Override
    public String toString() {
        return "MessageEvaluationResult[messageId=" + messageId + ", evaluatedGroups=" + evaluatedGroups
                + ", shortCircuited=" + shortCircuited + ", disclaimerMatches=" + disclaimerMatches
                + ", finalLexiconMatchesByFeatureId=" + finalLexiconMatchesByFeatureId
                + ", suppressedLexiconMatchCount=" + suppressedLexiconMatchCount + "]";
    }
}
