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
            + "\"trigger_type\":\"policy-alert-live\"}";

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
    }

    @Test
    @DisplayName("isLive()/isTest() correctly reflect the confirmed trigger_type values")
    void isLiveIsTest() throws Exception {
        RuntimeArgs live = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        assertThat(live.isLive()).isTrue();
        assertThat(live.isTest()).isFalse();

        RuntimeArgs test = new RuntimeArgs(live.datasetDetails(), live.featurePartitionValue(),
                live.pipelineExecId(), live.policyEngineId(), live.processId(), "policy-alert-test");
        assertThat(test.isTest()).isTrue();
        assertThat(test.isLive()).isFalse();
    }

    @Test
    @DisplayName("resolveMessageBucket picks the live bucket for a live trigger")
    void resolvesLiveBucket() throws Exception {
        RuntimeArgs live = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        ScanEngineProperties props = new ScanEngineProperties();
        props.setLiveMessageBucket("live-bucket");
        props.setTestMessageBucket("test-bucket");
        assertThat(props.resolveMessageBucket(live)).isEqualTo("live-bucket");
    }

    @Test
    @DisplayName("resolveMessageBucket picks the test bucket for a test trigger")
    void resolvesTestBucket() throws Exception {
        RuntimeArgs live = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        RuntimeArgs test = new RuntimeArgs(live.datasetDetails(), live.featurePartitionValue(),
                live.pipelineExecId(), live.policyEngineId(), live.processId(), "policy-alert-test");
        ScanEngineProperties props = new ScanEngineProperties();
        props.setLiveMessageBucket("live-bucket");
        props.setTestMessageBucket("test-bucket");
        assertThat(props.resolveMessageBucket(test)).isEqualTo("test-bucket");
    }

    @Test
    @DisplayName("resolveMessageBucket throws for an unrecognised trigger_type")
    void rejectsUnknownTriggerType() throws Exception {
        RuntimeArgs live = new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        RuntimeArgs bad = new RuntimeArgs(live.datasetDetails(), live.featurePartitionValue(),
                live.pipelineExecId(), live.policyEngineId(), live.processId(), "bogus");
        ScanEngineProperties props = new ScanEngineProperties();
        assertThatThrownBy(() -> props.resolveMessageBucket(bad)).isInstanceOf(IllegalArgumentException.class);
    }
}
