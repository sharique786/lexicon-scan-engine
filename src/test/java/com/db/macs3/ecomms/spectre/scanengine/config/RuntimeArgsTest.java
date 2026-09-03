package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RuntimeArgs / ScanEngineProperties")
class RuntimeArgsTest {

    private static final String SAMPLE_JSON = "{\"dataset_details\":[{\"dataset_id\":\"ds1\","
            + "\"dataset_partition_value\":\"p1\"}],\"feature_partition_value\":\"2026-08-16\","
            + "\"pipeline_exec_id\":\"pe-1\",\"policy_engine_id\":\"101\",\"process_id\":\"proc-1\","
            + "\"trigger_type\":\"policy-alert-live\",\"config_file_path\":\"gs://bucket/config.yml\"}";

    @Test
    @DisplayName("parses the requirement 2.a JSON shape correctly")
    void parsesRequirementJsonShape() throws Exception {
        RuntimeArgs ra = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        assertThat(ra.datasetDetails()).hasSize(1);
        assertThat(ra.datasetDetails().get(0).datasetId()).isEqualTo("ds1");
        assertThat(ra.featurePartitionValue()).isEqualTo("2026-08-16");
        assertThat(ra.pipelineExecId()).isEqualTo("pe-1");
        assertThat(ra.policyEngineId()).isEqualTo("101");
        assertThat(ra.processId()).isEqualTo("proc-1");
        assertThat(ra.configFilePath()).isEqualTo("gs://bucket/config.yml");
    }

    @Test
    @DisplayName("isLive()/isTest() correctly reflect the confirmed trigger_type values")
    void isLiveIsTest() throws Exception {
        RuntimeArgs live = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        assertThat(live.isLive()).isTrue();
        assertThat(live.isTest()).isFalse();

        RuntimeArgs test = new RuntimeArgs(live.datasetDetails(), live.featurePartitionValue(),
                live.pipelineExecId(), live.policyEngineId(), live.processId(), "policy-alert-test",
                live.configFilePath());
        assertThat(test.isTest()).isTrue();
        assertThat(test.isLive()).isFalse();
    }

    @Test
    @DisplayName("parseCliArgs parses the 7 --key=value Dataproc submit arguments")
    void parsesCliArgs() {
        String[] args = {
                "--process_id=913b68f9-0f62-4f51-a9c1-c9aa0d84c01c",
                "--pipeline_exec_id=2026-09-03_4-101",
                "--trigger_type=policy-alert-test",
                "--policy_engine_id=101",
                "--dataset_details=[{\"dataset_id\":\"006e3f06-045d-4f94-a9bd-780e603ef81f\","
                        + "\"dataset_partition_value\":\"2026-06-18\"}]",
                "--feature_partition_value=2026-07-16",
                "--config_file_path=gs://bucket/tmp/dataproc-config.yml",
        };

        RuntimeArgs runtimeArgs = RuntimeArgs.parseCliArgs(args);

        assertThat(runtimeArgs.processId()).isEqualTo("913b68f9-0f62-4f51-a9c1-c9aa0d84c01c");
        assertThat(runtimeArgs.pipelineExecId()).isEqualTo("2026-09-03_4-101");
        assertThat(runtimeArgs.triggerType()).isEqualTo("policy-alert-test");
        assertThat(runtimeArgs.policyEngineId()).isEqualTo("101");
        assertThat(runtimeArgs.featurePartitionValue()).isEqualTo("2026-07-16");
        assertThat(runtimeArgs.configFilePath()).isEqualTo("gs://bucket/tmp/dataproc-config.yml");
        assertThat(runtimeArgs.datasetDetails()).hasSize(1);
        assertThat(runtimeArgs.datasetDetails().get(0).datasetId())
                .isEqualTo("006e3f06-045d-4f94-a9bd-780e603ef81f");
        assertThat(runtimeArgs.datasetDetails().get(0).datasetPartitionValue()).isEqualTo("2026-06-18");
        assertThat(runtimeArgs.isTest()).isTrue();
    }

    @Test
    @DisplayName("parseCliArgs throws when a required argument is missing")
    void parseCliArgsThrowsOnMissingArgument() {
        String[] args = {
                "--process_id=p-1",
                "--pipeline_exec_id=pe-1",
                "--trigger_type=policy-alert-live",
                "--policy_engine_id=101",
                "--dataset_details=[{\"dataset_id\":\"ds1\",\"dataset_partition_value\":\"p1\"}]",
                "--feature_partition_value=2026-07-16",
                // --config_file_path deliberately omitted
        };
        assertThatThrownBy(() -> RuntimeArgs.parseCliArgs(args)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("parseCliArgs throws on a malformed --key=value argument")
    void parseCliArgsThrowsOnMalformedArgument() {
        String[] args = {"not-a-flag-argument"};
        assertThatThrownBy(() -> RuntimeArgs.parseCliArgs(args)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("parseCliArgs throws when --dataset_details isn't valid JSON")
    void parseCliArgsThrowsOnMalformedDatasetDetailsJson() {
        String[] args = {
                "--process_id=p-1",
                "--pipeline_exec_id=pe-1",
                "--trigger_type=policy-alert-live",
                "--policy_engine_id=101",
                "--dataset_details=not-json-at-all",
                "--feature_partition_value=2026-07-16",
                "--config_file_path=gs://bucket/config.yml",
        };
        assertThatThrownBy(() -> RuntimeArgs.parseCliArgs(args))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataset_details");
    }

    @Test
    @DisplayName("parseCliArgs handles several dataset_details entries (policy-alert-test shape)")
    void parseCliArgsHandlesMultipleDatasetDetails() {
        String[] args = {
                "--process_id=p-1",
                "--pipeline_exec_id=pe-1",
                "--trigger_type=policy-alert-test",
                "--policy_engine_id=101",
                "--dataset_details=[{\"dataset_id\":\"ds1\",\"dataset_partition_value\":\"p1\"},"
                        + "{\"dataset_id\":\"ds2\",\"dataset_partition_value\":\"p2\"}]",
                "--feature_partition_value=2026-07-16",
                "--config_file_path=gs://bucket/config.yml",
        };
        RuntimeArgs runtimeArgs = RuntimeArgs.parseCliArgs(args);
        assertThat(runtimeArgs.datasetDetails()).hasSize(2);
        assertThat(runtimeArgs.datasetDetails().get(1).datasetId()).isEqualTo("ds2");
    }

    @Test
    @DisplayName("equals/hashCode are value-based; toString carries every field")
    void equalsHashCodeAndToString() throws Exception {
        RuntimeArgs first = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        RuntimeArgs second = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second).isEqualTo(first);
        assertThat(first).isNotEqualTo(null);
        assertThat(first).isNotEqualTo("not a RuntimeArgs");

        RuntimeArgs differentTrigger = new RuntimeArgs(first.datasetDetails(), first.featurePartitionValue(),
                first.pipelineExecId(), first.policyEngineId(), first.processId(), "policy-alert-test",
                first.configFilePath());
        assertThat(first).isNotEqualTo(differentTrigger);

        assertThat(first.toString())
                .contains("proc-1")
                .contains("pe-1")
                .contains("101")
                .contains("policy-alert-live")
                .contains("gs://bucket/config.yml");
    }

    @Test
    @DisplayName("DatasetDetail equals/hashCode/toString are value-based")
    void datasetDetailEqualsHashCodeToString() {
        RuntimeArgs.DatasetDetail first = new RuntimeArgs.DatasetDetail("ds1", "p1");
        RuntimeArgs.DatasetDetail second = new RuntimeArgs.DatasetDetail("ds1", "p1");
        RuntimeArgs.DatasetDetail different = new RuntimeArgs.DatasetDetail("ds2", "p1");

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
        assertThat(first).isNotEqualTo(different);
        assertThat(first).isNotEqualTo(null);
        assertThat(first).isNotEqualTo("not a DatasetDetail");
        assertThat(first.toString()).contains("ds1").contains("p1");
    }
}
