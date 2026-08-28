package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One row of {@code feature-hit-summary} — per-message record of every
 * evaluated feature group and its hit/no-hit outcome, including each
 * composite/NoiseReduction group's individual sub-feature outcomes.
 *
 * <p>{@link Feature#hitStatus} is the GROUP's overall resolved hit status
 * (requirement 4's confirmed semantics: OR = any sub-feature hit is Yes;
 * AND = every sub-feature must hit for Yes) — see
 * {@code DecisionTreeEvaluator}'s {@code GroupEvaluationResult#isHit()}.
 * {@link SubFeature#hitStatus} is that ONE member's own individual hit
 * status, before the group's operator is applied.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class FeatureHitSummaryRow implements Serializable {

    private final String messageId;
    private final String datasetPartitionValue;
    private final String pipelineExecId;
    private final String processId;
    private final String featureHitType;
    private final List<Feature> features;
    private final String createdBy;
    private final Instant createdTs;

    /**
     * @param messageId                  the message this row is for
     * @param datasetPartitionValue      the Airflow-supplied partition this message's dataset was read under
     * @param pipelineExecId               the pipeline execution this row belongs to
     * @param processId                     the process run this row belongs to
     * @param featureHitType                carried verbatim from the view's {@code feature_tagging_type}
     *                                     column (e.g. {@code "Lexicon-Tagging"})
     * @param features                       one entry per evaluated feature group — only groups
     *                                     {@code DecisionTreeEvaluator} actually evaluated (a
     *                                     noise-reduction short-circuit means later groups are absent)
     * @param createdBy                      the writing job's identity
     * @param createdTs                       write time, UTC
     */
    public FeatureHitSummaryRow(String messageId, String datasetPartitionValue, String pipelineExecId,
                                 String processId, String featureHitType, List<Feature> features,
                                 String createdBy, Instant createdTs) {
        this.messageId = messageId;
        this.datasetPartitionValue = datasetPartitionValue;
        this.pipelineExecId = pipelineExecId;
        this.processId = processId;
        this.featureHitType = featureHitType;
        this.features = features;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
    }

    public String messageId() { return messageId; }
    public String datasetPartitionValue() { return datasetPartitionValue; }
    public String pipelineExecId() { return pipelineExecId; }
    public String processId() { return processId; }
    public String featureHitType() { return featureHitType; }
    public List<Feature> features() { return features; }
    public String createdBy() { return createdBy; }
    public Instant createdTs() { return createdTs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeatureHitSummaryRow)) return false;
        FeatureHitSummaryRow other = (FeatureHitSummaryRow) o;
        return Objects.equals(messageId, other.messageId)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(featureHitType, other.featureHitType)
                && Objects.equals(features, other.features)
                && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, datasetPartitionValue, pipelineExecId, processId,
                featureHitType, features, createdBy, createdTs);
    }

    @Override
    public String toString() {
        return "FeatureHitSummaryRow[messageId=" + messageId + ", datasetPartitionValue=" + datasetPartitionValue
                + ", pipelineExecId=" + pipelineExecId + ", processId=" + processId
                + ", featureHitType=" + featureHitType + ", features=" + features
                + ", createdBy=" + createdBy + ", createdTs=" + createdTs + "]";
    }

    public static final class Feature implements Serializable {

        private final long id;
        private final String name;
        private final String type;
        private final boolean isNoiseReduction;
        private final boolean hitStatus;
        private final List<SubFeature> subFeatures;

        /**
         * @param id                  the group's {@code feature_id}, parsed to an integer
         * @param name                 the group's {@code feature_name}
         * @param type                  the group's {@code feature_type}
         * @param isNoiseReduction     the group's {@code is_noise_reduction} flag
         * @param hitStatus             the group's OVERALL resolved hit status — see class Javadoc
         * @param subFeatures            one entry per member row; empty for a single-member group
         *                              (a standalone lexicon/disclaimer has nothing further to break down)
         */
        public Feature(long id, String name, String type, boolean isNoiseReduction,
                        boolean hitStatus, List<SubFeature> subFeatures) {
            this.id = id;
            this.name = name;
            this.type = type;
            this.isNoiseReduction = isNoiseReduction;
            this.hitStatus = hitStatus;
            this.subFeatures = subFeatures;
        }

        public long id() { return id; }
        public String name() { return name; }
        public String type() { return type; }
        public boolean isNoiseReduction() { return isNoiseReduction; }
        public boolean hitStatus() { return hitStatus; }
        public List<SubFeature> subFeatures() { return subFeatures; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Feature)) return false;
            Feature other = (Feature) o;
            return id == other.id && isNoiseReduction == other.isNoiseReduction && hitStatus == other.hitStatus
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

    public static final class SubFeature implements Serializable {

        private final String type;
        private final String name;
        private final boolean hitStatus;

        /**
         * @param type          the member row's {@code sub_feature_type}
         * @param name           the member row's {@code features_to_apply} — the actual lexicon
         *                       feature name this sub-feature applied
         * @param hitStatus      this ONE member's own hit status (any term match at all — see
         *                       {@code DecisionTreeEvaluator} class Javadoc on minimumHits)
         */
        public SubFeature(String type, String name, boolean hitStatus) {
            this.type = type;
            this.name = name;
            this.hitStatus = hitStatus;
        }

        public String type() { return type; }
        public String name() { return name; }
        public boolean hitStatus() { return hitStatus; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SubFeature)) return false;
            SubFeature other = (SubFeature) o;
            return hitStatus == other.hitStatus && Objects.equals(type, other.type) && Objects.equals(name, other.name);
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
