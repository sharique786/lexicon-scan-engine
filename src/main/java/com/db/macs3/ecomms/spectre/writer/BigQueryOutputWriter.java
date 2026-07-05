package com.db.macs3.ecomms.spectre.writer;

import com.db.macs3.ecomms.spectre.model.*;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Writes scan results to all five BigQuery tables owned/contributed-to by the
 * Lexicon Scan Engine:
 * <ul>
 *   <li>{@code lexicon-hit-summary}   (nested, one row per message)</li>
 *   <li>{@code lexicon-hit-restricted} (nested, only restricted messages with hits)</li>
 *   <li>{@code feature-hit-summary}   (flat, one row per evaluated feature/sub-feature)</li>
 *   <li>{@code pipeline_stage_audit}  (flat, one row per job execution)</li>
 *   <li>{@code pipeline_record_audit} (flat, one row per processed message)</li>
 * </ul>
 *
 * <p>All writes use the BigQuery Storage Write API
 * ({@code writeMethod=STORAGE_WRITE_API}) via the Spark BigQuery connector,
 * which natively supports nested {@code RECORD}/{@code REPEATED} schemas
 * derived from nested Java bean {@code List<T>} fields — no manual
 * {@link StructType} construction is required for the nested tables.
 */
@Component
public class BigQueryOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(BigQueryOutputWriter.class);
    private static final String WRITE_METHOD = "STORAGE_WRITE_API";

    /**
     * Extracts and writes {@code lexicon-hit-summary}, {@code lexicon-hit-restricted},
     * and {@code feature-hit-summary} from a single cached {@code Dataset<MessageScanResult>},
     * then writes {@code pipeline_record_audit}.
     *
     * @return the number of messages that had at least one lexicon hit
     */
    public long writeAllOutputs(SparkSession spark, Dataset<MessageScanResult> scanResults, JobConfig config) {
        long hitCount = writeLexiconHitSummary(scanResults, config);
        writeLexiconHitRestricted(scanResults, config);
        writeFeatureHitSummary(scanResults, config);
        writeRecordAudit(scanResults, config);
        return hitCount;
    }

    // ── lexicon-hit-summary ────────────────────────────────────────────────────

    private long writeLexiconHitSummary(Dataset<MessageScanResult> scanResults, JobConfig config) {
        Dataset<LexiconHitSummaryRow> summaryDS = scanResults.map(
                (MapFunction<MessageScanResult, LexiconHitSummaryRow>) MessageScanResult::getLexiconHitSummaryRow,
                Encoders.bean(LexiconHitSummaryRow.class)
        );

        long total   = summaryDS.count();
        long hitOnly = summaryDS
                .filter((FilterFunction<LexiconHitSummaryRow>) lexiconHitSummaryRow -> !lexiconHitSummaryRow.getEvaluatedLexicons().isEmpty())
                .count();
        log.info("Writing {} lexicon-hit-summary rows ({} with at least one hit) to {}",
                 total, hitOnly, config.getOutputTables().lexiconHitSummary);

        if (total == 0) {
            log.info("No messages processed — skipping write to lexicon-hit-summary");
            return 0;
        }

        summaryDS.write()
                 .format("bigquery")
                 .option("project", config.getBqProject())
                 .option("table", config.getOutputTables().lexiconHitSummary)
                 .option("writeMethod", WRITE_METHOD)
                 .option("partitionField", "run_date")
                 .mode(SaveMode.Append)
                 .save();

        log.info("lexicon-hit-summary write complete: {} rows", total);
        return hitOnly;
    }

    // ── lexicon-hit-restricted ─────────────────────────────────────────────────

    private void writeLexiconHitRestricted(Dataset<MessageScanResult> scanResults, JobConfig config) {
        Dataset<LexiconHitRestrictedRow> restrictedDS = scanResults
                .filter(MessageScanResult::hasRestrictedRow)
                .map((MapFunction<MessageScanResult, LexiconHitRestrictedRow>) MessageScanResult::getLexiconHitRestrictedRow,
                     Encoders.bean(LexiconHitRestrictedRow.class));

        long count = restrictedDS.count();
        log.info("Writing {} lexicon-hit-restricted rows to {}", count, config.getOutputTables().lexiconHitRestricted);

        if (count == 0) {
            log.info("No restricted-message hits — skipping write to lexicon-hit-restricted");
            return;
        }

        restrictedDS.write()
                    .format("bigquery")
                    .option("project", config.getBqProject())
                    .option("table", config.getOutputTables().lexiconHitRestricted)
                    .option("writeMethod", WRITE_METHOD)
                    .mode(SaveMode.Append)
                    .save();

        log.info("lexicon-hit-restricted write complete: {} rows", count);
    }

    // ── feature-hit-summary ────────────────────────────────────────────────────

    private void writeFeatureHitSummary(Dataset<MessageScanResult> scanResults, JobConfig config) {
        Dataset<FeatureHitSummaryRow> featureDS = scanResults.flatMap(
                (FlatMapFunction<MessageScanResult, FeatureHitSummaryRow>) r -> r.getFeatureHitSummaryRows().iterator(),
                Encoders.bean(FeatureHitSummaryRow.class)
        );

        long count = featureDS.count();
        log.info("Writing {} feature-hit-summary rows to {}", count, config.getOutputTables().featureHitSummary);

        if (count == 0) {
            log.info("No features evaluated — skipping write to feature-hit-summary");
            return;
        }

        featureDS.write()
                 .format("bigquery")
                 .option("project", config.getBqProject())
                 .option("table", config.getOutputTables().featureHitSummary)
                 .option("writeMethod", WRITE_METHOD)
                 .option("partitionField", "run_date")
                 .mode(SaveMode.Append)
                 .save();

        log.info("feature-hit-summary write complete: {} rows", count);
    }

    // ── pipeline_record_audit ──────────────────────────────────────────────────

    private void writeRecordAudit(Dataset<MessageScanResult> scanResults, JobConfig config) {
        Dataset<PipelineRecordAuditRow> recordAuditDS = scanResults.map(
                (MapFunction<MessageScanResult, PipelineRecordAuditRow>) r -> {
                    LexiconHitSummaryRow summary = r.getLexiconHitSummaryRow();
                    PipelineRecordAuditRow audit = new PipelineRecordAuditRow();
                    audit.setProcessId(summary.getProcessId());
                    audit.setPipelineExecId(summary.getPipelineExecId());
                    audit.setStageName(com.db.macs3.ecomms.spectre.model.ScanEngineArgs.STAGE_NAME);
                    audit.setRecordId(summary.getMessageId());
                    audit.setMsgOutputFileType(PipelineRecordAuditRow.OUTPUT_TYPE_LEXICON_TAGGING);
                    audit.setMsgOutputFileNm(config.getOutputTables().lexiconHitSummary);
                    audit.setOutputFile(config.bqViewRef());
                    audit.setStatus(PipelineRecordAuditRow.STATUS_SUCCESS);
                    audit.setCreatedTs(Timestamp.from(Instant.now()));
                    return audit;
                },
                Encoders.bean(PipelineRecordAuditRow.class)
        );

        long count = recordAuditDS.count();
        log.info("Writing {} pipeline_record_audit rows to {}", count, config.getOutputTables().pipelineRecordAudit);

        if (count == 0) return;

        recordAuditDS.write()
                     .format("bigquery")
                     .option("project", config.getBqProject())
                     .option("table", config.getOutputTables().pipelineRecordAudit)
                     .option("writeMethod", WRITE_METHOD)
                     .mode(SaveMode.Append)
                     .save();

        log.info("pipeline_record_audit write complete: {} rows", count);
    }

    // ── pipeline_stage_audit ───────────────────────────────────────────────────

    /**
     * Writes exactly one {@code pipeline_stage_audit} row for this job execution.
     * Called both on success and on failure (with {@code jobStatus=FAILED} and
     * a non-null {@code errorMessage}) so every run is auditable.
     */
    public void writeStageAudit(SparkSession spark, JobConfig config, ScanEngineArgs args, Timestamp jobStart,
                                 long inputCount, long recordCount, String jobStatus, String errorMessage) {
        log.info("Writing pipeline_stage_audit to {}", config.getOutputTables().pipelineStageAudit);

        StructType schema = new StructType()
                .add("process_id",         DataTypes.StringType, true)
                .add("trigger_type",       DataTypes.StringType, true)
                .add("eval_test_id",       DataTypes.StringType, true)
                .add("pipeline_exec_id",   DataTypes.StringType, true)
                .add("stage_name",         DataTypes.StringType, true)
                .add("compsr_dag_name",    DataTypes.StringType, true)
                .add("compsr_dag_path",    DataTypes.StringType, true)
                .add("dproc_script_name",  DataTypes.StringType, true)
                .add("dproc_script_path",  DataTypes.StringType, true)
                .add("start_time",         DataTypes.TimestampType, true)
                .add("end_time",           DataTypes.TimestampType, true)
                .add("job_status",         DataTypes.StringType, true)
                .add("input_file_count",   DataTypes.LongType, true)
                .add("output_file_count",  DataTypes.LongType, true)
                .add("record_cnt",         DataTypes.LongType, true)
                .add("error_count",        DataTypes.LongType, true)
                .add("error_message",      DataTypes.StringType, true);

        Timestamp endTime = Timestamp.from(Instant.now());
        long errorCount = errorMessage != null ? 1L : 0L;

        Dataset<org.apache.spark.sql.Row> auditDf = spark.createDataFrame(
                Collections.singletonList(RowFactory.create(
                        args.getProcessId(), args.getTriggerType(), args.getEvalTestId(), args.getPipelineExecId(),
                        com.db.macs3.ecomms.spectre.model.ScanEngineArgs.STAGE_NAME,
                        args.getCompsrDagName(), args.getCompsrDagPath(),
                        args.getDprocScriptName(), args.getDprocScriptPath(),
                        jobStart, endTime, jobStatus,
                        inputCount, 1L, recordCount, errorCount, errorMessage
                )),
                schema
        );

        auditDf.write()
               .format("bigquery")
               .option("project", config.getBqProject())
               .option("table", config.getOutputTables().pipelineStageAudit)
               .option("writeMethod", WRITE_METHOD)
               .mode(SaveMode.Append)
               .save();

        log.info("pipeline_stage_audit written: status={}", jobStatus);
    }
}
