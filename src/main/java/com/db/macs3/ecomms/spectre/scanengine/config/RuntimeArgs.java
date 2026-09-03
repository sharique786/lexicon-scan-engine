package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The Airflow (Google Composer) DAG's runtime parameters for one job
 * invocation.
 *
 * <h2>Supplied as 7 {@code --key=value} Dataproc submit arguments, not a
 * JSON file</h2>
 * <p>An earlier revision of this job took two positional GCS paths (a
 * {@code RuntimeArgs} JSON file and a {@code BqTableConfig} JSON file — see
 * {@link BqTableConfig} class Javadoc, since superseded). Composer now
 * submits 7 named arguments directly on the {@code spark-submit} command
 * line instead:
 *
 * <pre>
 * --process_id=913b68f9-0f62-4f51-a9c1-c9aa0d84c01c
 * --pipeline_exec_id=2026-09-03_4-101
 * --trigger_type=policy-alert-test
 * --policy_engine_id=101
 * --dataset_details=[{"dataset_id":"...","dataset_partition_value":"2026-06-18"}]
 * --feature_partition_value=2026-07-16
 * --config_file_path=gs://.../dataproc-config-....yml
 * </pre>
 *
 * <p>The first 6 map directly onto this class's fields ({@code dataset_details}
 * is a JSON array *inline* in its argument's value, parsed the same way it
 * always was — never a separate file). {@code config_file_path} is new: it
 * points to a YAML file on GCS (not JSON) carrying the BigQuery table/view
 * identifiers and the Hyperscan/message GCS bucket locations this run needs
 * — see {@link DataprocConfig} for that file's shape and
 * {@code ScanEngineJobRunner} for how it's read and applied. This class
 * stores that path but does not itself read the file — {@code RuntimeArgs}
 * stays a plain, GCS-independent data holder, same as every other field
 * here.
 *
 * <p>Use {@link #parseCliArgs(String[])} to build an instance from the raw
 * {@code String[] args} Dataproc/Spring hands {@code main}; the
 * {@link JsonCreator} constructor below remains for the (still exercised by
 * tests) case of constructing/parsing one directly from an equivalent JSON
 * body.
 *
 * <h2>{@code dataset_details} cardinality by trigger type</h2>
 * <p>For {@code policy-alert-live}, this list always has exactly one entry.
 * For {@code policy-alert-test}, it can have several — the engine reads/
 * queries once per entry and unions the results (see
 * {@code FeatureScanOrchestrator}). This class does not itself enforce
 * either cardinality; it is a plain, uniform data holder for both cases.
 *
 * <p>A plain class rather than a record, matching this project's other
 * pre-existing model classes.
 */
public final class RuntimeArgs implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<DatasetDetail> datasetDetails;
    private final String featurePartitionValue;
    private final String pipelineExecId;
    private final String policyEngineId;
    private final String processId;
    private final String triggerType;
    private final String configFilePath;

    /**
     * @param datasetDetails            one entry per dataset this run processes
     * @param featurePartitionValue     query parameter passed to the view — see
     *                                   {@code BqColumns.View#PARAM_FEATURE_PARTITION_VALUE}
     * @param pipelineExecId             this pipeline execution's identifier
     * @param policyEngineId              which policy engine's compiled features to use
     * @param processId                    this process run's identifier
     * @param triggerType                  {@code "policy-alert-live"} or {@code "policy-alert-test"}
     * @param configFilePath                GCS path to the {@link DataprocConfig} YAML file
     */
    @JsonCreator
    public RuntimeArgs(@JsonProperty("dataset_details") List<DatasetDetail> datasetDetails,
                        @JsonProperty("feature_partition_value") String featurePartitionValue,
                        @JsonProperty("pipeline_exec_id") String pipelineExecId,
                        @JsonProperty("policy_engine_id") String policyEngineId,
                        @JsonProperty("process_id") String processId,
                        @JsonProperty("trigger_type") String triggerType,
                        @JsonProperty("config_file_path") String configFilePath) {
        this.datasetDetails = datasetDetails;
        this.featurePartitionValue = featurePartitionValue;
        this.pipelineExecId = pipelineExecId;
        this.policyEngineId = policyEngineId;
        this.processId = processId;
        this.triggerType = triggerType;
        this.configFilePath = configFilePath;
    }

    /**
     * Parses the 7 {@code --key=value} Dataproc submit arguments described in
     * this class's Javadoc into a {@link RuntimeArgs}. {@code dataset_details}'
     * value is itself a JSON array, parsed via Jackson exactly as it would be
     * inside a larger JSON body.
     *
     * @throws IllegalArgumentException if any required argument is missing,
     *          blank, or malformed (via {@link CliArgumentParser})
     */
    public static RuntimeArgs parseCliArgs(String[] args) {
        Map<String, String> parsed = CliArgumentParser.parse(args);

        String datasetDetailsJson = CliArgumentParser.require(parsed, "dataset_details");
        List<DatasetDetail> datasetDetails;
        try {
            datasetDetails = MAPPER.readValue(datasetDetailsJson, new TypeReference<List<DatasetDetail>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Malformed --dataset_details JSON: " + datasetDetailsJson, e);
        }

        return new RuntimeArgs(
                datasetDetails,
                CliArgumentParser.require(parsed, "feature_partition_value"),
                CliArgumentParser.require(parsed, "pipeline_exec_id"),
                CliArgumentParser.require(parsed, "policy_engine_id"),
                CliArgumentParser.require(parsed, "process_id"),
                CliArgumentParser.require(parsed, "trigger_type"),
                CliArgumentParser.require(parsed, "config_file_path"));
    }

    public List<DatasetDetail> datasetDetails() { return datasetDetails; }
    public String featurePartitionValue() { return featurePartitionValue; }
    public String pipelineExecId() { return pipelineExecId; }
    public String policyEngineId() { return policyEngineId; }
    public String processId() { return processId; }
    public String triggerType() { return triggerType; }
    public String configFilePath() { return configFilePath; }

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
        if (this == o) {
            return true;
        }
        if (!(o instanceof RuntimeArgs)) {
            return false;
        }
        RuntimeArgs other = (RuntimeArgs) o;
        return Objects.equals(datasetDetails, other.datasetDetails)
                && Objects.equals(featurePartitionValue, other.featurePartitionValue)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(policyEngineId, other.policyEngineId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(triggerType, other.triggerType)
                && Objects.equals(configFilePath, other.configFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetDetails, featurePartitionValue, pipelineExecId, policyEngineId,
                processId, triggerType, configFilePath);
    }

    @Override
    public String toString() {
        return "RuntimeArgs[datasetDetails=" + datasetDetails + ", featurePartitionValue=" + featurePartitionValue
                + ", pipelineExecId=" + pipelineExecId + ", policyEngineId=" + policyEngineId
                + ", processId=" + processId + ", triggerType=" + triggerType
                + ", configFilePath=" + configFilePath + "]";
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
            if (this == o) {
                return true;
            }
            if (!(o instanceof DatasetDetail)) {
                return false;
            }
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
