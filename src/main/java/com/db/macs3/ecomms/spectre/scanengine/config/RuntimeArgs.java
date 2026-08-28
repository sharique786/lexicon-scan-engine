package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The Airflow (Google Composer) DAG's runtime parameters for one job
 * invocation — requirement 2.a's JSON shape, plus {@code triggerType} (shown
 * separately in the workflow diagram's "Runtime Args" as its own value, not
 * nested in this JSON body).
 *
 * <pre>
 * {
 *   "dataset_details": [{"dataset_id": "...", "dataset_partition_value": "..."}],
 *   "feature_partition_value": "2026-08-16",
 *   "pipeline_exec_id": "&lt;UUID&gt;",
 *   "policy_engine_id": "101",
 *   "process_id": "&lt;UUID&gt;"
 * }
 * </pre>
 *
 * <h2>{@code dataset_details} cardinality by trigger type</h2>
 * <p>Confirmed: for {@code policy-alert-live}, this list always has exactly
 * one entry. For {@code policy-alert-test}, it can have several — the
 * engine reads/queries once per entry and unions the results (see
 * {@code FeatureScanOrchestrator}). This class does not itself enforce
 * either cardinality; it is a plain, uniform data holder for both cases.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class RuntimeArgs implements Serializable {

    private final List<DatasetDetail> datasetDetails;
    private final String featurePartitionValue;
    private final String pipelineExecId;
    private final String policyEngineId;
    private final String processId;
    private final String triggerType;

    /**
     * @param datasetDetails            one entry per dataset this run processes
     * @param featurePartitionValue     query parameter passed to the view — see
     *                                   {@code BqColumns.View#PARAM_FEATURE_PARTITION_VALUE}
     * @param pipelineExecId             this pipeline execution's identifier
     * @param policyEngineId              which policy engine's compiled features to use
     * @param processId                    this process run's identifier
     * @param triggerType                  {@code "policy-alert-live"} / {@code "policy-alert-test"} —
     *                                   confirmed values, replacing the workflow diagram's earlier
     *                                   {@code POLICY_DEPLOYMENT}/{@code POLICY_TEST} naming
     */
    @JsonCreator
    public RuntimeArgs(@JsonProperty("dataset_details") List<DatasetDetail> datasetDetails,
                        @JsonProperty("feature_partition_value") String featurePartitionValue,
                        @JsonProperty("pipeline_exec_id") String pipelineExecId,
                        @JsonProperty("policy_engine_id") String policyEngineId,
                        @JsonProperty("process_id") String processId,
                        @JsonProperty("trigger_type") String triggerType) {
        this.datasetDetails = datasetDetails;
        this.featurePartitionValue = featurePartitionValue;
        this.pipelineExecId = pipelineExecId;
        this.policyEngineId = policyEngineId;
        this.processId = processId;
        this.triggerType = triggerType;
    }

    public List<DatasetDetail> datasetDetails() { return datasetDetails; }
    public String featurePartitionValue() { return featurePartitionValue; }
    public String pipelineExecId() { return pipelineExecId; }
    public String policyEngineId() { return policyEngineId; }
    public String processId() { return processId; }
    public String triggerType() { return triggerType; }

    public static final String TRIGGER_TYPE_LIVE = "policy-alert-live";
    public static final String TRIGGER_TYPE_TEST = "policy-alert-test";

    public boolean isLive() {
        return TRIGGER_TYPE_LIVE.equals(triggerType);
    }

    public boolean isTest() {
        return TRIGGER_TYPE_TEST.equals(triggerType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuntimeArgs)) return false;
        RuntimeArgs other = (RuntimeArgs) o;
        return Objects.equals(datasetDetails, other.datasetDetails)
                && Objects.equals(featurePartitionValue, other.featurePartitionValue)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(policyEngineId, other.policyEngineId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(triggerType, other.triggerType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetDetails, featurePartitionValue, pipelineExecId, policyEngineId,
                processId, triggerType);
    }

    @Override
    public String toString() {
        return "RuntimeArgs[datasetDetails=" + datasetDetails + ", featurePartitionValue=" + featurePartitionValue
                + ", pipelineExecId=" + pipelineExecId + ", policyEngineId=" + policyEngineId
                + ", processId=" + processId + ", triggerType=" + triggerType + "]";
    }

    public static final class DatasetDetail implements Serializable {

        private final String datasetId;
        private final String datasetPartitionValue;

        @JsonCreator
        public DatasetDetail(@JsonProperty("dataset_id") String datasetId,
                              @JsonProperty("dataset_partition_value") String datasetPartitionValue) {
            this.datasetId = datasetId;
            this.datasetPartitionValue = datasetPartitionValue;
        }

        public String datasetId() { return datasetId; }
        public String datasetPartitionValue() { return datasetPartitionValue; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DatasetDetail)) return false;
            DatasetDetail other = (DatasetDetail) o;
            return Objects.equals(datasetId, other.datasetId)
                    && Objects.equals(datasetPartitionValue, other.datasetPartitionValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datasetId, datasetPartitionValue);
        }

        @Override
        public String toString() {
            return "DatasetDetail[datasetId=" + datasetId + ", datasetPartitionValue=" + datasetPartitionValue + "]";
        }
    }
}
