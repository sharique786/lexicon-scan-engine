package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link PipelineRecordAuditRowMapper}'s own {@code call} logic —
 * no {@code SparkSession} is needed since {@code OutputTableWriter.toRow}
 * builds a plain {@link Row} via {@code RowFactory.create}, not anything
 * Spark-session-dependent. See {@link SparkSessionConfigTest}/
 * {@link PipelineRecordAuditIntegrationTest} for coverage that actually runs
 * this class through a real {@code Dataset.mapPartitions}.
 */
@DisplayName("PipelineRecordAuditRowMapper")
class PipelineRecordAuditRowMapperTest {

    private static final LocalDate EXECUTION_DATE = LocalDate.parse("2026-09-08");

    private static final String SAMPLE_JSON = "{\"dataset_details\":[{\"dataset_id\":\"ds1\","
            + "\"dataset_partition_value\":\"p1\"}],\"feature_partition_value\":\"2026-08-16\","
            + "\"pipeline_exec_id\":\"pipe-1\",\"policy_engine_id\":\"policy-1\",\"process_id\":\"proc-1\","
            + "\"trigger_type\":\"policy-alert-test\",\"config_file_path\":\"gs://bucket/config.yml\"}";

    private static RuntimeArgs runtimeArgs() {
        try {
            return new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Row> mapAll(List<MessageProcessingResult> input) {
        PipelineRecordAuditRowMapper mapper =
                new PipelineRecordAuditRowMapper(runtimeArgs(), "lexicon-scan-engine", "scan-engine", EXECUTION_DATE);
        Iterator<Row> output = mapper.call(input.iterator());
        List<Row> rows = new ArrayList<>();
        output.forEachRemaining(rows::add);
        return rows;
    }

    @Test
    @DisplayName("emits one row per input result, for BOTH success and failure — none are skipped")
    void emitsOneRowPerResultRegardlessOfOutcome() {
        List<MessageProcessingResult> input = List.of(
                MessageProcessingResult.success("msg-1", true, "2026-09-08", null, null, null),
                MessageProcessingResult.failure("msg-2", false, "2026-09-08", "boom"),
                MessageProcessingResult.success("msg-3", true, "2026-09-08", null, null, null));

        assertThat(mapAll(input)).hasSize(3);
    }

    @Test
    @DisplayName("a successful result gets status=SUCCESS, return_code=0, error_message=null")
    void successRowFields() {
        MessageProcessingResult success = MessageProcessingResult.success(
                "msg-101", true, "2026-09-08", null, null, null);

        Row row = mapAll(List.of(success)).getFirst();

        assertThat(row.getString(11)).isEqualTo(BqColumns.RecordStatus.SUCCESS);
        assertThat(row.getInt(12)).isZero();
        assertThat(row.get(13)).isNull();
    }

    @Test
    @DisplayName("a failed result gets status=FAILED, return_code=1, error_message populated from the result")
    void failureRowFields() {
        MessageProcessingResult failure = MessageProcessingResult.failure(
                "msg-102", false, "2026-09-08", "java.lang.IllegalStateException: bad message");

        Row row = mapAll(List.of(failure)).getFirst();

        assertThat(row.getString(11)).isEqualTo(BqColumns.RecordStatus.FAILED);
        assertThat(row.getInt(12)).isEqualTo(1);
        assertThat(row.getString(13)).isEqualTo("java.lang.IllegalStateException: bad message");
    }

    @Test
    @DisplayName("record_id is the result's own message_id, not any other identifier")
    void recordIdIsMessageId() {
        MessageProcessingResult result = MessageProcessingResult.success(
                "msg-xyz-789", true, "2026-09-08", null, null, null);

        Row row = mapAll(List.of(result)).getFirst();

        assertThat(row.getString(4)).isEqualTo("msg-xyz-789");
    }

    @Test
    @DisplayName("process_id/trigger_type/pipeline_exec_id come from RuntimeArgs, not the constructor's own fields")
    void populatesFromRuntimeArgs() {
        MessageProcessingResult result = MessageProcessingResult.success(
                "msg-1", true, "2026-09-08", null, null, null);

        Row row = mapAll(List.of(result)).getFirst();

        assertThat(row.getString(0)).isEqualTo("proc-1");
        assertThat(row.getString(1)).isEqualTo("policy-alert-test");
        assertThat(row.getString(3)).isEqualTo("pipe-1");
    }

    @Test
    @DisplayName("stage_name/created_by/execution_date come from the constructor's own fields, not RuntimeArgs")
    void populatesFromConstructorFields() {
        MessageProcessingResult result = MessageProcessingResult.success(
                "msg-1", true, "2026-09-08", null, null, null);

        Row row = mapAll(List.of(result)).getFirst();

        assertThat(row.getString(5)).isEqualTo("lexicon-scan-engine");
        assertThat(row.getString(25)).isEqualTo("scan-engine");
        assertThat(row.getDate(27).toLocalDate()).isEqualTo(EXECUTION_DATE);
    }

    @Test
    @DisplayName("an empty input partition produces no rows")
    void emptyInputProducesNoRows() {
        assertThat(mapAll(List.of())).isEmpty();
    }
}
