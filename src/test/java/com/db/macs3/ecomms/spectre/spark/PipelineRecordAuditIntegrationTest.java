package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.OutputTableWriter;
import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: runs {@link PipelineRecordAuditRowMapper} through a REAL
 * {@code Dataset<MessageProcessingResult>.mapPartitions(...)} call (not a bare
 * {@code Iterator}, see {@link PipelineRecordAuditRowMapperTest} for that) via
 * a local-mode {@link SparkSession}, then applies the SAME
 * {@code dropDuplicates(record_id, stage_name, execution_date,
 * pipeline_exec_id)} step {@code ScanEngineJobRunner.writeOutputs} applies —
 * reproducing the exact transformation chain that fixed the duplicate-row bug,
 * short of the final BigQuery write itself (which needs live GCP credentials
 * — see README "Known limitations"). This also incidentally proves the Rows
 * {@link PipelineRecordAuditRowMapper} builds are genuinely compatible with
 * {@code Encoders.row(OutputTableWriter.PIPELINE_RECORD_AUDIT_SCHEMA)} — a
 * schema/type mismatch here would fail at {@code collectAsList()} even though
 * a bare-mapper unit test would never catch it.
 */
@DisplayName("pipeline_record_audit: mapper + dedup, through a real Dataset")
class PipelineRecordAuditIntegrationTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        System.setProperty("spark.master", "local[2]");
        System.setProperty("spark.ui.enabled", "false");
        spark = new SparkSessionConfig().sparkSession();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        System.clearProperty("spark.master");
        System.clearProperty("spark.ui.enabled");
    }

    private static RuntimeArgs runtimeArgs() {
        return new RuntimeArgs(List.of(), null, "pipe-1", "policy-1", "proc-1", "policy-alert-test", null);
    }

    private Dataset<Row> buildRecordAuditRows(List<MessageProcessingResult> results) {
        Dataset<MessageProcessingResult> resultsDs = spark.createDataset(results, Encoders.kryo(MessageProcessingResult.class));
        return resultsDs.mapPartitions(
                new PipelineRecordAuditRowMapper(runtimeArgs(), "lexicon-scan-engine", "scan-engine",
                        LocalDate.parse("2026-09-08")),
                Encoders.row(OutputTableWriter.PIPELINE_RECORD_AUDIT_SCHEMA));
    }

    private static Dataset<Row> dedupOnNaturalKey(Dataset<Row> recordAuditRows) {
        return recordAuditRows.dropDuplicates(
                BqColumns.PipelineRecordAudit.RECORD_ID, BqColumns.PipelineRecordAudit.STAGE_NAME,
                BqColumns.PipelineRecordAudit.EXECUTION_DATE, BqColumns.PipelineRecordAudit.PIPELINE_EXEC_ID);
    }

    @Test
    @DisplayName("with no duplicate message_ids upstream, every result still gets its own row")
    void noDuplicatesMeansNoRowsDropped() {
        List<MessageProcessingResult> results = List.of(
                MessageProcessingResult.success("msg-1", true, "2026-09-08", null, null, null),
                MessageProcessingResult.failure("msg-2", false, "2026-09-08", "boom"),
                MessageProcessingResult.success("msg-3", true, "2026-09-08", null, null, null));

        Dataset<Row> recordAuditRows = buildRecordAuditRows(results);
        Dataset<Row> deduped = dedupOnNaturalKey(recordAuditRows);

        assertThat(recordAuditRows.count()).isEqualTo(3);
        assertThat(deduped.count()).isEqualTo(3);
    }

    @Test
    @DisplayName("a message_id appearing more than once upstream collapses to exactly one audit row")
    void duplicateMessageIdsCollapseAfterDedup() {
        // Simulates the real-world cause: the message+view join fans one message_id out into
        // more than one row in `results`, so PipelineRecordAuditRowMapper (correctly) emits one
        // pipeline_record_audit row per occurrence — the dedup step is what collapses those back
        // down to one row per actual record.
        List<MessageProcessingResult> results = List.of(
                MessageProcessingResult.success("msg-1", true, "2026-09-08", null, null, null),
                MessageProcessingResult.success("msg-1", true, "2026-09-08", null, null, null),
                MessageProcessingResult.failure("msg-2", false, "2026-09-08", "boom"),
                MessageProcessingResult.failure("msg-2", false, "2026-09-08", "boom"),
                MessageProcessingResult.success("msg-3", true, "2026-09-08", null, null, null));

        Dataset<Row> recordAuditRows = buildRecordAuditRows(results);
        Dataset<Row> deduped = dedupOnNaturalKey(recordAuditRows);

        assertThat(recordAuditRows.count()).isEqualTo(5);
        assertThat(deduped.count()).isEqualTo(3);

        Map<String, String> statusByRecordId = deduped.collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getAs("record_id"), row -> row.getAs("status")));
        assertThat(statusByRecordId)
                .containsEntry("msg-1", BqColumns.RecordStatus.SUCCESS)
                .containsEntry("msg-2", BqColumns.RecordStatus.FAILED)
                .containsEntry("msg-3", BqColumns.RecordStatus.SUCCESS);
    }
}
