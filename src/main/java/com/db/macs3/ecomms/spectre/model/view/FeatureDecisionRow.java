package com.db.macs3.ecomms.spectre.model.view;

import com.db.macs3.ecomms.spectre.constants.BqColumns;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDate;
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
 * <p><b>Data types rechecked against the live view (this revision):</b>
 * {@link #getDatasetPartition}/{@link #getFeaturePartitionValue} are DATE
 * columns, and {@link #getFeatureId} is a LONG (INTEGER) column — NOT plain
 * STRING, despite the rest of this row being string-typed. {@link ViewRowConverter
 * ViewRowConverter} previously read every column via {@code row.getString(...)},
 * which throws {@code ClassCastException} for these three at real-query time
 * (never caught by the unit tests, which construct this class directly rather
 * than through a real Spark {@code Row}). {@link #getFeatureId} stays a
 * {@code Long} here — {@code FeatureGroupingService} converts it to the
 * {@code String} {@link com.db.macs3.ecomms.spectre.model.decision.FeatureGroup#getFeatureId()}
 * needs (that field's own delivered schema, confirmed separately, is STRING)
 * at the one point that conversion is needed; every field in this class
 * stays a faithful, unconverted mirror of the view's own column type.
 */
public class FeatureDecisionRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String processId;
    private String messageId;
    private LocalDate datasetPartition;
    private String featureTaggingType;
    private String featureType;
    private Long featureId;
    private String featureName;
    private String subFeatureType;
    private String featuresToApply;
    private String isNoiseReduction;
    private String operator;
    private String featureDefinitionJson;
    private LocalDate featurePartitionValue;
    private String policyEngineId;

    /**
     * @param processId              the process run this row belongs to
     * @param messageId               joins to the AVRO message dataset's {@code message_id}
     * @param datasetPartition        the view's own partition column (distinct from the
     *                                 Airflow-supplied {@code dataset_partition_value} used to
     *                                 query the view — see {@code FeatureDecisionViewReader}); DATE
     * @param featureTaggingType      e.g. {@code "Lexicon-Tagging"} — carried through to
     *                                 {@code feature-hit-summary.feature_hit_type} verbatim
     * @param featureType              {@link com.db.macs3.ecomms.spectre.constants.BqColumns.FeatureType} —
     *                                 {@code lexicon}, {@code composite}, {@code disclaimer}, or {@code NoiseReduction}
     * @param featureId                groups rows belonging to the same (possibly composite) feature —
     *                                 see {@code FeatureGroupingService}; LONG (INTEGER) in the view,
     *                                 not STRING
     * @param featureName              the feature's display name (parent name for a composite grouping)
     * @param subFeatureType           non-null (currently always {@code "lexicon"}) when this row is
     *                                 one sub-feature of a composite/NoiseReduction grouping
     * @param featuresToApply          the actual lexicon feature name for THIS row — this is what
     *                                 gets looked up inside {@code feature_definition.body.lexiconName}
     *                                 to resolve the {@code .hdb} filename, NOT {@code featureName}
     *                                 (which may be a composite parent's display label)
     * @param isNoiseReduction         {@code "Y"} / {@code "N"} — string, not boolean, matching the
     *                                 view's own column type
     * @param operator                 {@code "OR"} / {@code "AND"} / null — combines sibling rows
     *                                 sharing the same {@code featureId} (composite/NoiseReduction only)
     * @param featureDefinitionJson    raw JSON string — parsed on demand via
     *                                 {@link com.db.macs3.ecomms.spectre.model.feature.FeatureDefinition#parse}
     * @param featurePartitionValue    the feature-master partition this row was tagged under; DATE
     * @param policyEngineId           the policy engine this feature belongs to
     */
    public FeatureDecisionRow(String processId, String messageId, LocalDate datasetPartition,
                               String featureTaggingType, String featureType, Long featureId,
                               String featureName, String subFeatureType, String featuresToApply,
                               String isNoiseReduction, String operator, String featureDefinitionJson,
                               LocalDate featurePartitionValue, String policyEngineId) {
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

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public LocalDate getDatasetPartition() {
        return datasetPartition;
    }

    public void setDatasetPartition(LocalDate datasetPartition) {
        this.datasetPartition = datasetPartition;
    }

    public String getFeatureTaggingType() {
        return featureTaggingType;
    }

    public void setFeatureTaggingType(String featureTaggingType) {
        this.featureTaggingType = featureTaggingType;
    }

    public String getFeatureType() {
        return featureType;
    }

    public void setFeatureType(String featureType) {
        this.featureType = featureType;
    }

    public Long getFeatureId() {
        return featureId;
    }

    public void setFeatureId(Long featureId) {
        this.featureId = featureId;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public String getSubFeatureType() {
        return subFeatureType;
    }

    public void setSubFeatureType(String subFeatureType) {
        this.subFeatureType = subFeatureType;
    }

    public String getFeaturesToApply() {
        return featuresToApply;
    }

    public void setFeaturesToApply(String featuresToApply) {
        this.featuresToApply = featuresToApply;
    }

    public String getIsNoiseReduction() {
        return isNoiseReduction;
    }

    public void setIsNoiseReduction(String isNoiseReduction) {
        this.isNoiseReduction = isNoiseReduction;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getFeatureDefinitionJson() {
        return featureDefinitionJson;
    }

    public void setFeatureDefinitionJson(String featureDefinitionJson) {
        this.featureDefinitionJson = featureDefinitionJson;
    }

    public LocalDate getFeaturePartitionValue() {
        return featurePartitionValue;
    }

    public void setFeaturePartitionValue(LocalDate featurePartitionValue) {
        this.featurePartitionValue = featurePartitionValue;
    }

    public String getPolicyEngineId() {
        return policyEngineId;
    }

    public void setPolicyEngineId(String policyEngineId) {
        this.policyEngineId = policyEngineId;
    }

    /** @return true iff {@link #getIsNoiseReduction} is exactly {@code "Y"} (case-sensitive, matches upstream). */
    public boolean isNoiseReductionFlag() {
        return BqColumns.YES.equals(isNoiseReduction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FeatureDecisionRow)) {
            return false;
        }
        FeatureDecisionRow other = (FeatureDecisionRow) obj;
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
