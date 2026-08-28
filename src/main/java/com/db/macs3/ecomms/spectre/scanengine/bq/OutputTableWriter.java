package com.db.macs3.ecomms.spectre.scanengine.bq;

import com.db.macs3.ecomms.spectre.scanengine.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.scanengine.model.output.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts each of the 6 output/audit row records into a Spark
 * {@code Dataset<Row>} against an explicit {@link StructType} schema, then
 * writes it to BigQuery via the Spark BigQuery connector
 * ({@code write().format("bigquery")}, append mode — every write here adds
 * new rows; this job never updates rows in place).
 *
 * <p>Explicit {@code Row}/{@code StructType} construction is used rather
 * than a bean/reflection-based encoder because these are Java records (no
 * zero-arg constructor + setters for {@code Encoders.bean} to use) — this
 * also keeps the exact BigQuery column names ({@code BqColumns}) decoupled
 * from Java field-naming conventions.
 *
 * <p>Every write here is a normal Spark action on an already-distributed
 * {@code Dataset} — no {@code collect()}, no driver-side row construction at
 * message scale; {@link #toDataset} runs its {@code map} entirely on
 * executors.
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox — see {@code GcsClient} class Javadoc.
 */
public final class OutputTableWriter {

    private OutputTableWriter() {}

    // ── lexicon-hit-summary ──────────────────────────────────────────────────

    private static final StructType TERM_DTL_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("term_id", DataTypes.StringType, false),
            DataTypes.createStructField("term_regex_pattern", DataTypes.StringType, true),
            DataTypes.createStructField("regex_match_hit_count", DataTypes.LongType, false),
    });
    private static final StructType EVALUATED_LEXICON_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("total_terms_count", DataTypes.LongType, false),
            DataTypes.createStructField("regex_hit_count", DataTypes.LongType, false),
            DataTypes.createStructField("term_dtls", DataTypes.createArrayType(TERM_DTL_SUMMARY_TYPE), false),
    });
    public static final StructType LEXICON_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("message_id", DataTypes.StringType, false),
            DataTypes.createStructField("process_id", DataTypes.StringType, false),
            DataTypes.createStructField("pipeline_exec_id", DataTypes.StringType, false),
            DataTypes.createStructField("evaluated_lexicons", DataTypes.createArrayType(EVALUATED_LEXICON_SUMMARY_TYPE), false),
            DataTypes.createStructField("created_by", DataTypes.StringType, false),
            DataTypes.createStructField("created_ts", DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitSummaryRow r) {
        List<Row> lexicons = r.evaluatedLexicons().stream().map(el -> RowFactory.create(
                el.id(), el.name(), el.totalTermsCount(), el.regexHitCount(),
                el.termDtls().stream().map(td -> RowFactory.create(
                        td.termId(), td.termRegexPattern(), td.regexMatchHitCount())).collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(r.messageId(), r.processId(), r.pipelineExecId(), lexicons,
                r.createdBy(), Timestamp.from(r.createdTs()));
    }

    public static void writeLexiconHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> ds = spark.createDataFrame(rows, LEXICON_HIT_SUMMARY_SCHEMA);
        writeAppend(ds, config.fullyQualifiedTable(config.lexiconHitSummaryTable()));
    }

    // ── lexicon-hit-restricted / lexicon-hit-unrestricted (shared schema) ───

    private static final StructType TERM_DTL_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("term_id", DataTypes.StringType, false),
            DataTypes.createStructField("matched_text", DataTypes.StringType, false), // JSON column type: string content
    });
    private static final StructType EVALUATED_LEXICON_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false),
            DataTypes.createStructField("term_dtls", DataTypes.createArrayType(TERM_DTL_DETAIL_TYPE), false),
    });
    public static final StructType LEXICON_HIT_DETAIL_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("message_id", DataTypes.StringType, false),
            DataTypes.createStructField("process_id", DataTypes.StringType, false),
            DataTypes.createStructField("pipeline_exec_id", DataTypes.StringType, false),
            DataTypes.createStructField("dataset_partition_value", DataTypes.StringType, false),
            DataTypes.createStructField("evaluated_lexicons", DataTypes.createArrayType(EVALUATED_LEXICON_DETAIL_TYPE), false),
            DataTypes.createStructField("created_by", DataTypes.StringType, false),
            DataTypes.createStructField("created_ts", DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitDetailRow r) {
        List<Row> lexicons = r.evaluatedLexicons().stream().map(el -> RowFactory.create(
                el.id(), el.termDtls().stream().map(td -> RowFactory.create(
                        td.termId(), td.matchedText())).collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(r.messageId(), r.processId(), r.pipelineExecId(), r.datasetPartitionValue(),
                lexicons, r.createdBy(), Timestamp.from(r.createdTs()));
    }

    public static void writeLexiconHitDetail(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows, boolean restricted) {
        Dataset<Row> ds = spark.createDataFrame(rows, LEXICON_HIT_DETAIL_SCHEMA);
        String tableName = restricted ? config.lexiconHitRestrictedTable() : config.lexiconHitUnrestrictedTable();
        writeAppend(ds, config.fullyQualifiedTable(tableName));
    }

    // ── feature-hit-summary ──────────────────────────────────────────────────

    private static final StructType SUB_FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("type", DataTypes.StringType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("hit_status", DataTypes.BooleanType, false),
    });
    private static final StructType FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("type", DataTypes.StringType, true),
            DataTypes.createStructField("is_noise_reduction", DataTypes.BooleanType, false),
            DataTypes.createStructField("hit_status", DataTypes.BooleanType, false),
            DataTypes.createStructField("sub_features", DataTypes.createArrayType(SUB_FEATURE_TYPE), false),
    });
    public static final StructType FEATURE_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("message_id", DataTypes.StringType, false),
            DataTypes.createStructField("dataset_partition_value", DataTypes.StringType, false),
            DataTypes.createStructField("pipeline_exec_id", DataTypes.StringType, false),
            DataTypes.createStructField("process_id", DataTypes.StringType, false),
            DataTypes.createStructField("feature_hit_type", DataTypes.StringType, true),
            DataTypes.createStructField("features", DataTypes.createArrayType(FEATURE_TYPE), false),
            DataTypes.createStructField("created_by", DataTypes.StringType, false),
            DataTypes.createStructField("created_ts", DataTypes.TimestampType, false),
    });

    public static Row toRow(FeatureHitSummaryRow r) {
        List<Row> features = r.features().stream().map(f -> RowFactory.create(
                f.id(), f.name(), f.type(), f.isNoiseReduction(), f.hitStatus(),
                f.subFeatures().stream().map(sf -> RowFactory.create(
                        sf.type(), sf.name(), sf.hitStatus())).collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(r.messageId(), r.datasetPartitionValue(), r.pipelineExecId(), r.processId(),
                r.featureHitType(), features, r.createdBy(), Timestamp.from(r.createdTs()));
    }

    public static void writeFeatureHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> ds = spark.createDataFrame(rows, FEATURE_HIT_SUMMARY_SCHEMA);
        writeAppend(ds, config.fullyQualifiedTable(config.featureHitSummaryTable()));
    }

    // ── pipeline_stage_audit ─────────────────────────────────────────────────

    public static final StructType PIPELINE_STAGE_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("process_id", DataTypes.StringType, false),
            DataTypes.createStructField("trigger_type", DataTypes.StringType, false),
            DataTypes.createStructField("pipelinex_exec_id", DataTypes.StringType, false),
            DataTypes.createStructField("stage_name", DataTypes.StringType, false),
            DataTypes.createStructField("composer_dag_name", DataTypes.StringType, true),
            DataTypes.createStructField("composer_dag_path", DataTypes.StringType, true),
            DataTypes.createStructField("dproc_dag_name", DataTypes.StringType, true),
            DataTypes.createStructField("dproc_dag_path", DataTypes.StringType, true),
            DataTypes.createStructField("start_time", DataTypes.TimestampType, true),
            DataTypes.createStructField("end_time", DataTypes.TimestampType, true),
            DataTypes.createStructField("job_status", DataTypes.StringType, false),
            DataTypes.createStructField("error_count", DataTypes.StringType, true),
            DataTypes.createStructField("error_message", DataTypes.StringType, true),
            DataTypes.createStructField("additional_info", DataTypes.StringType, true),
            DataTypes.createStructField("execution_date", DataTypes.DateType, false),
    });

    public static Row toRow(PipelineStageAuditRow r) {
        return RowFactory.create(r.processId(), r.triggerType(), r.pipelineExecId(), r.stageName(),
                r.composerDagName(), r.composerDagPath(), r.dprocDagName(), r.dprocDagPath(),
                r.startTime() == null ? null : Timestamp.from(r.startTime()),
                r.endTime() == null ? null : Timestamp.from(r.endTime()),
                r.jobStatus(), r.errorCount(), r.errorMessage(), r.additionalInfo(),
                java.sql.Date.valueOf(r.executionDate()));
    }

    public static void writePipelineStageAudit(SparkSession spark, BqTableConfig config, PipelineStageAuditRow row) {
        // Single-row audit write — a normal driver-side write of ONE small row is not a
        // scaling concern (contrast the per-message tables above, always written from an
        // already-distributed Dataset).
        Dataset<Row> ds = spark.createDataFrame(List.of(toRow(row)), PIPELINE_STAGE_AUDIT_SCHEMA);
        writeAppend(ds, config.fullyQualifiedTable(config.pipelineStageAuditTable()));
    }

    // ── pipeline_record_audit ────────────────────────────────────────────────

    public static final StructType PIPELINE_RECORD_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("process_id", DataTypes.StringType, false),
            DataTypes.createStructField("trigger_type", DataTypes.StringType, false),
            DataTypes.createStructField("pipelinex_exec_id", DataTypes.StringType, false),
            DataTypes.createStructField("stage_name", DataTypes.StringType, false),
            DataTypes.createStructField("record_id", DataTypes.StringType, false),
            DataTypes.createStructField("status", DataTypes.StringType, false),
            DataTypes.createStructField("return_code", DataTypes.IntegerType, true),
            DataTypes.createStructField("error_message", DataTypes.StringType, true),
            DataTypes.createStructField("execution_date", DataTypes.DateType, false),
            DataTypes.createStructField("created_by", DataTypes.StringType, false),
            DataTypes.createStructField("created_ts", DataTypes.TimestampType, false),
    });

    public static Row toRow(PipelineRecordAuditRow r) {
        return RowFactory.create(r.processId(), r.triggerType(), r.pipelineExecId(), r.stageName(),
                r.recordId(), r.status(), r.returnCode(), r.errorMessage(),
                java.sql.Date.valueOf(r.executionDate()), r.createdBy(), Timestamp.from(r.createdTs()));
    }

    public static void writePipelineRecordAudit(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> ds = spark.createDataFrame(rows, PIPELINE_RECORD_AUDIT_SCHEMA);
        writeAppend(ds, config.fullyQualifiedTable(config.pipelineRecordAuditTable()));
    }

    // ── shared write path ────────────────────────────────────────────────────

    private static void writeAppend(Dataset<Row> ds, String fullyQualifiedTable) {
        ds.write()
                .format("bigquery")
                .option("table", fullyQualifiedTable)
                .mode(SaveMode.Append)
                .save();
    }
}
