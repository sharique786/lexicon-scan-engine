package com.db.macs3.ecomms.spectre.model.decision;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * All {@link FeatureDecisionRow}s for one message sharing the same
 * {@code featureId} — a single standalone lexicon/disclaimer feature, or a
 * composite/NoiseReduction feature grouping several lexicon sub-features
 * combined by {@link #getOperator}.
 *
 * <p>See {@code FeatureGroupingService} for how raw view rows are grouped
 * into these, and {@code DecisionTreeEvaluator} for how they are evaluated
 * (NoiseReduction → Disclaimer → Lexicon, with a noise-reduction
 * short-circuit rule).
 */
public class FeatureGroup implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String featureId;
    private String featureName;
    private String featureType;
    private boolean isNoiseReduction;
    private String operator;
    private List<FeatureDecisionRow> members;

    /**
     * @param featureId          groups {@link #getMembers} — all share this value
     * @param featureName         the (possibly composite/parent) display name
     * @param featureType          {@link BqColumns.FeatureType} value
     * @param isNoiseReduction     true iff ANY member row is flagged {@code is_noise_reduction=Y} —
     *                              in practice all members of one group share the same flag,
     *                              but this does not assume that
     * @param operator              {@code OR}/{@code AND}, combining {@link #getMembers} when there is
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

    public String getFeatureId() {
        return featureId;
    }

    public void setFeatureId(String featureId) {
        this.featureId = featureId;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public String getFeatureType() {
        return featureType;
    }

    public void setFeatureType(String featureType) {
        this.featureType = featureType;
    }

    public boolean isNoiseReduction() {
        return isNoiseReduction;
    }

    public void setNoiseReduction(boolean noiseReduction) {
        this.isNoiseReduction = noiseReduction;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public List<FeatureDecisionRow> getMembers() {
        return members;
    }

    public void setMembers(List<FeatureDecisionRow> members) {
        this.members = members;
    }

    /** @return true iff {@link #getFeatureType} is {@code disclaimer} (case-insensitive). */
    public boolean isDisclaimer() {
        return BqColumns.FeatureType.DISCLAIMER.equalsIgnoreCase(featureType);
    }

    /** @return true iff {@link #getMembers} has more than one row, i.e. {@link #getOperator} is meaningful. */
    public boolean isMultiMember() {
        return members.size() > 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FeatureGroup)) {
            return false;
        }
        FeatureGroup other = (FeatureGroup) obj;
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
