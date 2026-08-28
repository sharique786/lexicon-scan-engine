package com.db.macs3.ecomms.spectre.scanengine.model.view;

import java.io.Serializable;
import java.util.Objects;

/**
 * One row of {@code vw_src_msg_lexicon_decision_mapping} — a single
 * (message_id, feature) pairing the decision engine must evaluate. A message
 * with multiple applicable features (composite sub-features, several
 * standalone lexicons, a disclaimer) produces multiple rows sharing the same
 * {@code messageId}.
 *
 * <p>{@link Serializable} — this class travels inside Spark's shuffle/join
 * machinery (grouped by {@code messageId} against the AVRO message dataset),
 * so every field must itself be serialisable; all fields here are.
 *
 * <p>Java 11 class (not a record — this project targets Java 11) exposing
 * the same accessor-method-per-field shape a record would, so every call
 * site reads identically to before.
 */
public final class FeatureDecisionRow implements Serializable {

    private final String processId;
    private final String messageId;
    private final String datasetPartition;
    private final String featureTaggingType;
    private final String featureType;
    private final String featureId;
    private final String featureName;
    private final String subFeatureType;
    private final String featuresToApply;
    private final String isNoiseReduction;
    private final String operator;
    private final String featureDefinitionJson;
    private final String featurePartitionValue;
    private final String policyEngineId;

    /**
     * @param processId              the process run this row belongs to
     * @param messageId               joins to the AVRO message dataset's {@code message_id}
     * @param datasetPartition        the view's own partition column (distinct from the
     *                                 Airflow-supplied {@code dataset_partition_value} used to
     *                                 query the view — see {@code FeatureDecisionViewReader})
     * @param featureTaggingType      e.g. {@code "Lexicon-Tagging"} — carried through to
     *                                 {@code feature-hit-summary.feature_hit_type} verbatim
     * @param featureType              {@link com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns.FeatureType} —
     *                                 {@code lexicon}, {@code composite}, {@code disclaimer}, or {@code NoiseReduction}
     * @param featureId                groups rows belonging to the same (possibly composite) feature —
     *                                 see {@code FeatureGroupingService}
     * @param featureName              the feature's display name (parent name for a composite grouping)
     * @param subFeatureType           non-null (currently always {@code "lexicon"}) when this row is
     *                                 one sub-feature of a composite/NoiseReduction grouping
     * @param featuresToApply          the actual lexicon feature name for THIS row — this is what
     *                                 gets looked up inside {@code feature_definition.body.feature}
     *                                 to resolve the {@code .hdb} filename, NOT {@code featureName}
     *                                 (which may be a composite parent's display label)
     * @param isNoiseReduction         {@code "Y"} / {@code "N"} — string, not boolean, matching the
     *                                 view's own column type
     * @param operator                 {@code "OR"} / {@code "AND"} / null — combines sibling rows
     *                                 sharing the same {@code featureId} (composite/NoiseReduction only)
     * @param featureDefinitionJson    raw JSON string — parsed on demand via
     *                                 {@link com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition#parse}
     * @param featurePartitionValue    the feature-master partition this row was tagged under
     * @param policyEngineId           the policy engine this feature belongs to
     */
    public FeatureDecisionRow(String processId, String messageId, String datasetPartition,
                               String featureTaggingType, String featureType, String featureId,
                               String featureName, String subFeatureType, String featuresToApply,
                               String isNoiseReduction, String operator, String featureDefinitionJson,
                               String featurePartitionValue, String policyEngineId) {
        this.processId = processId;
        this.messageId = messageId;
        this.datasetPartition = datasetPartition;
        this.featureTaggingType = featureTaggingType;
        this.featureType = featureType;
        this.featureId = featureId;
        this.featureName = featureName;
        this.subFeatureType = subFeatureType;
        this.featuresToApply = featuresToApply;
        this.isNoiseReduction = isNoiseReduction;
        this.operator = operator;
        this.featureDefinitionJson = featureDefinitionJson;
        this.featurePartitionValue = featurePartitionValue;
        this.policyEngineId = policyEngineId;
    }

    public String processId() { return processId; }
    public String messageId() { return messageId; }
    public String datasetPartition() { return datasetPartition; }
    public String featureTaggingType() { return featureTaggingType; }
    public String featureType() { return featureType; }
    public String featureId() { return featureId; }
    public String featureName() { return featureName; }
    public String subFeatureType() { return subFeatureType; }
    public String featuresToApply() { return featuresToApply; }
    public String isNoiseReduction() { return isNoiseReduction; }
    public String operator() { return operator; }
    public String featureDefinitionJson() { return featureDefinitionJson; }
    public String featurePartitionValue() { return featurePartitionValue; }
    public String policyEngineId() { return policyEngineId; }

    /** @return true iff {@link #isNoiseReduction} is exactly {@code "Y"} (case-sensitive, matches upstream). */
    public boolean isNoiseReductionFlag() {
        return "Y".equals(isNoiseReduction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeatureDecisionRow)) return false;
        FeatureDecisionRow other = (FeatureDecisionRow) o;
        return Objects.equals(processId, other.processId)
                && Objects.equals(messageId, other.messageId)
                && Objects.equals(datasetPartition, other.datasetPartition)
                && Objects.equals(featureTaggingType, other.featureTaggingType)
                && Objects.equals(featureType, other.featureType)
                && Objects.equals(featureId, other.featureId)
                && Objects.equals(featureName, other.featureName)
                && Objects.equals(subFeatureType, other.subFeatureType)
                && Objects.equals(featuresToApply, other.featuresToApply)
                && Objects.equals(isNoiseReduction, other.isNoiseReduction)
                && Objects.equals(operator, other.operator)
                && Objects.equals(featureDefinitionJson, other.featureDefinitionJson)
                && Objects.equals(featurePartitionValue, other.featurePartitionValue)
                && Objects.equals(policyEngineId, other.policyEngineId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, messageId, datasetPartition, featureTaggingType, featureType,
                featureId, featureName, subFeatureType, featuresToApply, isNoiseReduction, operator,
                featureDefinitionJson, featurePartitionValue, policyEngineId);
    }

    @Override
    public String toString() {
        return "FeatureDecisionRow[processId=" + processId + ", messageId=" + messageId
                + ", datasetPartition=" + datasetPartition + ", featureTaggingType=" + featureTaggingType
                + ", featureType=" + featureType + ", featureId=" + featureId + ", featureName=" + featureName
                + ", subFeatureType=" + subFeatureType + ", featuresToApply=" + featuresToApply
                + ", isNoiseReduction=" + isNoiseReduction + ", operator=" + operator
                + ", featureDefinitionJson=" + featureDefinitionJson
                + ", featurePartitionValue=" + featurePartitionValue + ", policyEngineId=" + policyEngineId + "]";
    }
}
