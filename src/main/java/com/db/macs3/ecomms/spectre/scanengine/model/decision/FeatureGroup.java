package com.db.macs3.ecomms.spectre.scanengine.model.decision;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * All {@link FeatureDecisionRow}s for one message sharing the same
 * {@code featureId} — a single standalone lexicon/disclaimer feature, or a
 * composite/NoiseReduction feature grouping several lexicon sub-features
 * combined by {@link #operator}.
 *
 * <p>See {@code FeatureGroupingService} for how raw view rows are grouped
 * into these, and {@code DecisionTreeEvaluator} for how they are evaluated
 * in order (NoiseReduction → Disclaimer → Lexicon — requirement 2.f) with
 * the noise-reduction short-circuit rule (requirement 2.g).
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class FeatureGroup implements Serializable {

    private final String featureId;
    private final String featureName;
    private final String featureType;
    private final boolean isNoiseReduction;
    private final String operator;
    private final List<FeatureDecisionRow> members;

    /**
     * @param featureId          groups {@link #members} — all share this value
     * @param featureName         the (possibly composite/parent) display name
     * @param featureType          {@link BqColumns.FeatureType} value
     * @param isNoiseReduction     true iff ANY member row is flagged {@code is_noise_reduction=Y} —
     *                              in practice all members of one group share the same flag,
     *                              but this does not assume that
     * @param operator              {@code OR}/{@code AND}, combining {@link #members} when there is
     *                              more than one — null for a single-member group, where it is
     *                              meaningless (that one member's own hit status IS the group's)
     * @param members                one row per sub-feature actually applied under this
     *                              {@code featureId} — each row's {@code featuresToApply} names the
     *                              lexicon feature to load/scan for that member
     */
    public FeatureGroup(String featureId, String featureName, String featureType,
                         boolean isNoiseReduction, String operator, List<FeatureDecisionRow> members) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("FeatureGroup requires at least one member row for featureId=" + featureId);
        }
        this.featureId = featureId;
        this.featureName = featureName;
        this.featureType = featureType;
        this.isNoiseReduction = isNoiseReduction;
        this.operator = operator;
        this.members = members;
    }

    public String featureId() { return featureId; }
    public String featureName() { return featureName; }
    public String featureType() { return featureType; }
    public boolean isNoiseReduction() { return isNoiseReduction; }
    public String operator() { return operator; }
    public List<FeatureDecisionRow> members() { return members; }

    /** @return true iff {@link #featureType} is {@code disclaimer} (case-insensitive). */
    public boolean isDisclaimer() {
        return BqColumns.FeatureType.DISCLAIMER.equalsIgnoreCase(featureType);
    }

    /** @return true iff {@link #members} has more than one row, i.e. {@link #operator} is meaningful. */
    public boolean isMultiMember() {
        return members.size() > 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeatureGroup)) return false;
        FeatureGroup other = (FeatureGroup) o;
        return isNoiseReduction == other.isNoiseReduction
                && Objects.equals(featureId, other.featureId)
                && Objects.equals(featureName, other.featureName)
                && Objects.equals(featureType, other.featureType)
                && Objects.equals(operator, other.operator)
                && Objects.equals(members, other.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureId, featureName, featureType, isNoiseReduction, operator, members);
    }

    @Override
    public String toString() {
        return "FeatureGroup[featureId=" + featureId + ", featureName=" + featureName
                + ", featureType=" + featureType + ", isNoiseReduction=" + isNoiseReduction
                + ", operator=" + operator + ", members=" + members + "]";
    }
}
