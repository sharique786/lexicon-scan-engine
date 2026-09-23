package com.db.macs3.ecomms.spectre.model.decision;

import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The complete decision-tree outcome for one message — every group that was evaluated (in processing order),
 * the disclaimer matches, and the Lexicon-category matches that survive disclaimer suppression.
 *
 * <p>See {@code DecisionTreeEvaluator} for how this is produced: groups are evaluated in
 * {@code FeatureGroupingService}'s order (NoiseReduction → Disclaimer → Lexicon); if a NoiseReduction group is a
 * hit, evaluation stops there and {@link #isShortCircuited} is true — every LATER group is never evaluated at
 * all, not evaluated-and-discarded.
 *
 * <p>Output rows are built from different parts of this result: {@code lexicon-hit-summary} and
 * {@code feature-hit-summary} from {@link #getEvaluatedGroups}; {@code lexicon-hit-restricted}/{@code -unrestricted}
 * from the hit NoiseReduction and Disclaimer groups in {@code evaluatedGroups} plus
 * {@link #getFinalLexiconMatchesByFeatureId}.
 */
public class MessageEvaluationResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private List<GroupEvaluationResult> evaluatedGroups;
    private boolean shortCircuited;
    private List<TermMatchResult> disclaimerMatches;
    private Map<Long, List<TermMatchResult>> finalLexiconMatchesByFeatureId;
    private int suppressedLexiconMatchCount;

    /**
     * @param messageId                      the message this result is for
     * @param evaluatedGroups                every group that WAS evaluated, in processing
     *                                       order — stops early (per {@link #isShortCircuited})
     * @param shortCircuited                 true iff a NoiseReduction group was a hit,
     *                                       meaning Disclaimer and Lexicon groups were
     *                                       never evaluated at all
     * @param disclaimerMatches              every disclaimer match found (empty if there was
     *                                       no disclaimer group, or it had no matches) — the spans
     *                                       used to suppress Lexicon matches; disclaimer matches
     *                                       themselves are never suppressed
     * @param finalLexiconMatchesByFeatureId Lexicon-category matches AFTER disclaimer suppression (a
     *                                       match is dropped only when fully contained in a disclaimer
     *                                       match in the same area — see {@code DecisionTreeEvaluator}),
     *                                       keyed by the group's {@code featureId}. Disclaimer and
     *                                       NoiseReduction groups are never in this map. Empty when
     *                                       {@link #isShortCircuited} is true. A {@code featureId} with no
     *                                       surviving match is absent, not present with an empty list.
     * @param suppressedLexiconMatchCount    how many raw Lexicon matches were discarded by
     *                                       disclaimer-overlap suppression, for observability/audit —
     *                                       not itself written to any output table
     */
    public MessageEvaluationResult(String messageId, List<GroupEvaluationResult> evaluatedGroups,
                                   boolean shortCircuited, List<TermMatchResult> disclaimerMatches,
                                   Map<Long, List<TermMatchResult>> finalLexiconMatchesByFeatureId,
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

    public Map<Long, List<TermMatchResult>> getFinalLexiconMatchesByFeatureId() {
        return finalLexiconMatchesByFeatureId;
    }

    public void setFinalLexiconMatchesByFeatureId(Map<Long, List<TermMatchResult>> finalLexiconMatchesByFeatureId) {
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
        if (obj == null || this.getClass() != obj.getClass()) {
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
