package com.db.macs3.ecomms.spectre.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * One row of {@code feature-hit-summary} — per-message record of every
 * evaluated feature group and its hit/no-hit outcome, including each
 * composite/NoiseReduction group's individual sub-feature outcomes.
 *
 * <p>{@link Feature#getHitStatus} is the GROUP's overall resolved hit status
 * (OR = any sub-feature hit is Yes; AND = every sub-feature must hit for
 * Yes) — see {@code DecisionTreeEvaluator}'s {@code GroupEvaluationResult#isHit()}.
 * {@link SubFeature#getHitStatus} is that ONE member's own individual hit
 * status, before the group's operator is applied.
 *
 * <p>Field order and NOT NULL/NULLABLE mode match the delivered BigQuery
 * schema verbatim (rechecked against the live table) — notably different
 * from {@link LexiconHitSummaryRow}/{@link LexiconHitDetailRow}'s ordering:
 * {@code features} precedes {@code dataset_partition_value}, and
 * {@code process_id}/{@code pipeline_exec_id} come LAST, after
 * {@code created_by}/{@code created_ts}. Do not "fix" this to match the
 * other two tables' field order. {@link Feature#getIsNoiseReduction}/
 * {@link Feature#getHitStatus}/{@link SubFeature#getHitStatus} are NULLABLE
 * (hence boxed {@link Boolean}, not primitive {@code boolean}), and
 * {@link #getCreatedBy}/{@link #getCreatedTs} are NULLABLE here — unlike the
 * other two tables, where {@code created_ts} is REQUIRED.
 */
public class FeatureHitSummaryRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private List<Feature> features;
    private LocalDate datasetPartitionValue;
    private String featureHitType;
    private String createdBy;
    private Instant createdTs;
    private String processId;
    private String pipelineExecId;

    /**
     * @param messageId                  the message this row is for
     * @param features                       one entry per evaluated feature group — only groups
     *                                     {@code DecisionTreeEvaluator} actually evaluated (a
     *                                     noise-reduction short-circuit means later groups are absent)
     * @param datasetPartitionValue      {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for the
     *                                    dataset this message came from — see {@code ScanMessage} class Javadoc
     * @param featureHitType                carried verbatim from the view's {@code feature_tagging_type}
     *                                     column (e.g. {@code "Lexicon-Tagging"})
     * @param createdBy                      the writing job's identity; NULLABLE per the delivered schema
     * @param createdTs                       write time, UTC; NULLABLE per the delivered schema
     * @param processId                     the process run this row belongs to
     * @param pipelineExecId               the pipeline execution this row belongs to
     */
    public FeatureHitSummaryRow(String messageId, List<Feature> features, LocalDate datasetPartitionValue,
                                 String featureHitType, String createdBy, Instant createdTs,
                                 String processId, String pipelineExecId) {
        this.messageId = messageId;
        this.features = features;
        this.datasetPartitionValue = datasetPartitionValue;
        this.featureHitType = featureHitType;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
        this.processId = processId;
        this.pipelineExecId = pipelineExecId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    public void setFeatures(List<Feature> features) {
        this.features = features;
    }

    public LocalDate getDatasetPartitionValue() {
        return datasetPartitionValue;
    }

    public void setDatasetPartitionValue(LocalDate datasetPartitionValue) {
        this.datasetPartitionValue = datasetPartitionValue;
    }

    public String getFeatureHitType() {
        return featureHitType;
    }

    public void setFeatureHitType(String featureHitType) {
        this.featureHitType = featureHitType;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Instant getCreatedTs() {
        return createdTs;
    }

    public void setCreatedTs(Instant createdTs) {
        this.createdTs = createdTs;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getPipelineExecId() {
        return pipelineExecId;
    }

    public void setPipelineExecId(String pipelineExecId) {
        this.pipelineExecId = pipelineExecId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FeatureHitSummaryRow)) {
            return false;
        }
        FeatureHitSummaryRow other = (FeatureHitSummaryRow) obj;
        return Objects.equals(messageId, other.messageId)
                && Objects.equals(features, other.features)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(featureHitType, other.featureHitType)
                && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs)
                && Objects.equals(processId, other.processId)
                && Objects.equals(pipelineExecId, other.pipelineExecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, features, datasetPartitionValue, featureHitType, createdBy, createdTs,
                processId, pipelineExecId);
    }

    @Override
    public String toString() {
        return "FeatureHitSummaryRow[messageId=" + messageId + ", features=" + features
                + ", datasetPartitionValue=" + datasetPartitionValue + ", featureHitType=" + featureHitType
                + ", createdBy=" + createdBy + ", createdTs=" + createdTs
                + ", processId=" + processId + ", pipelineExecId=" + pipelineExecId + "]";
    }

    public static class Feature implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private long id;
        private String name;
        private String type;
        private Boolean isNoiseReduction;
        private Boolean hitStatus;
        private List<SubFeature> subFeatures;

        /**
         * @param id                  the group's {@code feature_id}, parsed to an integer
         * @param name                 the group's {@code feature_name}
         * @param type                  the group's {@code feature_type}
         * @param isNoiseReduction     the group's {@code is_noise_reduction} flag; NULLABLE per the
         *                              delivered schema
         * @param hitStatus             the group's OVERALL resolved hit status — see class Javadoc;
         *                              NULLABLE per the delivered schema
         * @param subFeatures            one entry per member row; empty for a single-member group
         *                              (a standalone lexicon/disclaimer has nothing further to break down)
         */
        public Feature(long id, String name, String type, Boolean isNoiseReduction,
                        Boolean hitStatus, List<SubFeature> subFeatures) {
            this.id = id;
            this.name = name;
            this.type = type;
            this.isNoiseReduction = isNoiseReduction;
            this.hitStatus = hitStatus;
            this.subFeatures = subFeatures;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Boolean getIsNoiseReduction() {
            return isNoiseReduction;
        }

        public void setIsNoiseReduction(Boolean noiseReduction) {
            isNoiseReduction = noiseReduction;
        }

        public Boolean getHitStatus() {
            return hitStatus;
        }

        public void setHitStatus(Boolean hitStatus) {
            this.hitStatus = hitStatus;
        }

        public List<SubFeature> getSubFeatures() {
            return subFeatures;
        }

        public void setSubFeatures(List<SubFeature> subFeatures) {
            this.subFeatures = subFeatures;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Feature)) {
                return false;
            }
            Feature other = (Feature) obj;
            return id == other.id
                    && Objects.equals(isNoiseReduction, other.isNoiseReduction)
                    && Objects.equals(hitStatus, other.hitStatus)
                    && Objects.equals(name, other.name) && Objects.equals(type, other.type)
                    && Objects.equals(subFeatures, other.subFeatures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, type, isNoiseReduction, hitStatus, subFeatures);
        }

        @Override
        public String toString() {
            return "Feature[id=" + id + ", name=" + name + ", type=" + type
                    + ", isNoiseReduction=" + isNoiseReduction + ", hitStatus=" + hitStatus
                    + ", subFeatures=" + subFeatures + "]";
        }
    }

    public static class SubFeature implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String type;
        private String name;
        private Boolean hitStatus;

        /**
         * @param type          the member row's {@code sub_feature_type}
         * @param name           the member row's {@code features_to_apply} — the actual lexicon
         *                       feature name this sub-feature applied
         * @param hitStatus      this ONE member's own hit status (any term match at all — see
         *                       {@code DecisionTreeEvaluator} class Javadoc on minimumHits);
         *                       NULLABLE per the delivered schema
         */
        public SubFeature(String type, String name, Boolean hitStatus) {
            this.type = type;
            this.name = name;
            this.hitStatus = hitStatus;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getHitStatus() {
            return hitStatus;
        }

        public void setHitStatus(Boolean hitStatus) {
            this.hitStatus = hitStatus;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SubFeature)) {
                return false;
            }
            SubFeature other = (SubFeature) obj;
            return Objects.equals(hitStatus, other.hitStatus) && Objects.equals(type, other.type)
                    && Objects.equals(name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, name, hitStatus);
        }

        @Override
        public String toString() {
            return "SubFeature[type=" + type + ", name=" + name + ", hitStatus=" + hitStatus + "]";
        }
    }
}
