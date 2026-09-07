package com.db.macs3.ecomms.spectre.model.decision;

import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;

import java.io.Serial;
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
 * stops there and {@link #isShortCircuited} is true — every LATER group
 * (further NoiseReduction groups, the Disclaimer group, all Lexicon groups)
 * is simply never evaluated at all, not evaluated-and-discarded.
 */
public class MessageEvaluationResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private List<GroupEvaluationResult> evaluatedGroups;
    private boolean shortCircuited;
    private List<TermMatchResult> disclaimerMatches;
    private Map<String, List<TermMatchResult>> finalLexiconMatchesByFeatureId;
    private int suppressedLexiconMatchCount;

    /**
     * @param messageId                        the message this result is for
     * @param evaluatedGroups                   every group that WAS evaluated, in processing
     *                                           order — stops early (per {@link #isShortCircuited})
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
     *                                           {@code featureId} (i.e. by {@link FeatureGroup#getFeatureId()})
     *                                           so a downstream row builder can reconstruct which
     *                                           evaluated group each surviving match belongs to — this
     *                                           is what {@code lexicon-hit-summary}/{@code -restricted}/
     *                                           {@code -unrestricted} are built from; empty when
     *                                           {@link #isShortCircuited} is true. A {@code featureId} with
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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public List<GroupEvaluationResult> getEvaluatedGroups() {
        return evaluatedGroups;
    }

    public void setEvaluatedGroups(List<GroupEvaluationResult> evaluatedGroups) {
        this.evaluatedGroups = evaluatedGroups;
    }

    public boolean isShortCircuited() {
        return shortCircuited;
    }

    public void setShortCircuited(boolean shortCircuited) {
        this.shortCircuited = shortCircuited;
    }

    public List<TermMatchResult> getDisclaimerMatches() {
        return disclaimerMatches;
    }

    public void setDisclaimerMatches(List<TermMatchResult> disclaimerMatches) {
        this.disclaimerMatches = disclaimerMatches;
    }

    public Map<String, List<TermMatchResult>> getFinalLexiconMatchesByFeatureId() {
        return finalLexiconMatchesByFeatureId;
    }

    public void setFinalLexiconMatchesByFeatureId(Map<String, List<TermMatchResult>> finalLexiconMatchesByFeatureId) {
        this.finalLexiconMatchesByFeatureId = finalLexiconMatchesByFeatureId;
    }

    public int getSuppressedLexiconMatchCount() {
        return suppressedLexiconMatchCount;
    }

    public void setSuppressedLexiconMatchCount(int suppressedLexiconMatchCount) {
        this.suppressedLexiconMatchCount = suppressedLexiconMatchCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MessageEvaluationResult)) {
            return false;
        }
        MessageEvaluationResult other = (MessageEvaluationResult) obj;
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
