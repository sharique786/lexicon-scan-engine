package com.db.macs3.ecomms.spectre.scanengine.bq;

import com.db.macs3.ecomms.spectre.scanengine.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
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
 * also keeps the exact BigQuery column names ({@link BqColumns}) decoupled
 * from Java field-naming conventions.
 *
 * <p>Every write here is a normal Spark action on an already-distributed
 * {@code Dataset} — no {@code collect()}, no driver-side row construction at
 * message scale; {@code toDataset}-style conversions run their {@code map}
 * entirely on executors.
 */
public final class OutputTableWriter {

    private OutputTableWriter() {}

    // ── lexicon-hit-summary ──────────────────────────────────────────────────

    private static final StructType TERM_DTL_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.TERM_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.TERM_REGEX_PATTERN, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.REGEX_MATCH_HIT_COUNT, DataTypes.LongType, false),
    });
    private static final StructType EVALUATED_LEXICON_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.TOTAL_TERMS_COUNT, DataTypes.LongType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.REGEX_HIT_COUNT, DataTypes.LongType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.TERM_DTLS,
                    DataTypes.createArrayType(TERM_DTL_SUMMARY_TYPE), false),
    });
    public static final StructType LEXICON_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_SUMMARY_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitSummaryRow summaryRow) {
        List<Row> lexicons = summaryRow.evaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.id(), lexicon.name(), lexicon.totalTermsCount(), lexicon.regexHitCount(),
                lexicon.termDtls().stream().map(termDtl -> RowFactory.create(
                        termDtl.termId(), termDtl.termRegexPattern(), termDtl.regexMatchHitCount()))
                        .collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.messageId(), summaryRow.processId(), summaryRow.pipelineExecId(),
                lexicons, summaryRow.createdBy(), Timestamp.from(summaryRow.createdTs()));
    }

    public static void writeLexiconHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, LEXICON_HIT_SUMMARY_SCHEMA);
        writeAppend(dataset, config.bqOutputHitSummary());
    }

    // ── lexicon-hit-restricted / lexicon-hit-unrestricted (shared schema) ───

    private static final StructType TERM_DTL_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.TermDtl.TERM_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.TermDtl.MATCHED_TEXT, DataTypes.StringType, false), // JSON column type: string content
    });
    private static final StructType EVALUATED_LEXICON_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EvaluatedLexicon.ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EvaluatedLexicon.TERM_DTLS,
                    DataTypes.createArrayType(TERM_DTL_DETAIL_TYPE), false),
    });
    public static final StructType LEXICON_HIT_DETAIL_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.DATASET_PARTITION_VALUE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_DETAIL_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitDetailRow detailRow) {
        List<Row> lexicons = detailRow.evaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.id(), lexicon.termDtls().stream().map(termDtl -> RowFactory.create(
                        termDtl.termId(), termDtl.matchedText())).collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(detailRow.messageId(), detailRow.processId(), detailRow.pipelineExecId(),
                detailRow.datasetPartitionValue(), lexicons, detailRow.createdBy(), Timestamp.from(detailRow.createdTs()));
    }

    public static void writeLexiconHitDetail(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows, boolean restricted) {
        Dataset<Row> dataset = spark.createDataFrame(rows, LEXICON_HIT_DETAIL_SCHEMA);
        String fullyQualifiedTable = restricted ? config.bqOutputHitRestricted() : config.bqOutputHitUnrestricted();
        writeAppend(dataset, fullyQualifiedTable);
    }

    // ── feature-hit-summary ──────────────────────────────────────────────────

    private static final StructType SUB_FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.HIT_STATUS, DataTypes.BooleanType, false),
    });
    private static final StructType FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.ID, DataTypes.LongType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.IS_NOISE_REDUCTION, DataTypes.BooleanType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.HIT_STATUS, DataTypes.BooleanType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.SUB_FEATURES,
                    DataTypes.createArrayType(SUB_FEATURE_TYPE), false),
    });
    public static final StructType FEATURE_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.DATASET_PARTITION_VALUE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURE_HIT_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURES, DataTypes.createArrayType(FEATURE_TYPE), false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(FeatureHitSummaryRow summaryRow) {
        List<Row> features = summaryRow.features().stream().map(feature -> RowFactory.create(
                feature.id(), feature.name(), feature.type(), feature.isNoiseReduction(), feature.hitStatus(),
                feature.subFeatures().stream().map(subFeature -> RowFactory.create(
                        subFeature.type(), subFeature.name(), subFeature.hitStatus())).collect(Collectors.toList())
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.messageId(), summaryRow.datasetPartitionValue(), summaryRow.pipelineExecId(),
                summaryRow.processId(), summaryRow.featureHitType(), features, summaryRow.createdBy(),
                Timestamp.from(summaryRow.createdTs()));
    }

    public static void writeFeatureHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, FEATURE_HIT_SUMMARY_SCHEMA);
        writeAppend(dataset, config.bqOutputFeatureHitSummary());
    }

    // ── pipeline_stage_audit ─────────────────────────────────────────────────

    public static final StructType PIPELINE_STAGE_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineStageAudit.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.TRIGGER_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.STAGE_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.COMPOSER_DAG_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.COMPOSER_DAG_PATH, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.DPROC_DAG_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.DPROC_DAG_PATH, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.START_TIME, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.END_TIME, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.JOB_STATUS, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ERROR_COUNT, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ERROR_MESSAGE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ADDITIONAL_INFO, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.EXECUTION_DATE, DataTypes.DateType, false),
    });

    public static Row toRow(PipelineStageAuditRow auditRow) {
        return RowFactory.create(auditRow.processId(), auditRow.triggerType(), auditRow.pipelineExecId(), auditRow.stageName(),
                auditRow.composerDagName(), auditRow.composerDagPath(), auditRow.dprocDagName(), auditRow.dprocDagPath(),
                auditRow.startTime() == null ? null : Timestamp.from(auditRow.startTime()),
                auditRow.endTime() == null ? null : Timestamp.from(auditRow.endTime()),
                auditRow.jobStatus(), auditRow.errorCount(), auditRow.errorMessage(), auditRow.additionalInfo(),
                java.sql.Date.valueOf(auditRow.executionDate()));
    }

    public static void writePipelineStageAudit(SparkSession spark, BqTableConfig config, PipelineStageAuditRow row) {
        // Single-row audit write — a normal driver-side write of ONE small row is not a
        // scaling concern (contrast the per-message tables above, always written from an
        // already-distributed Dataset).
        Dataset<Row> dataset = spark.createDataFrame(List.of(toRow(row)), PIPELINE_STAGE_AUDIT_SCHEMA);
        writeAppend(dataset, config.bqOutputStageAudit());
    }

    // ── pipeline_record_audit ────────────────────────────────────────────────

    public static final StructType PIPELINE_RECORD_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.TRIGGER_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.STAGE_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RECORD_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.STATUS, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RETURN_CODE, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.ERROR_MESSAGE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.EXECUTION_DATE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(PipelineRecordAuditRow auditRow) {
        return RowFactory.create(auditRow.processId(), auditRow.triggerType(), auditRow.pipelineExecId(), auditRow.stageName(),
                auditRow.recordId(), auditRow.status(), auditRow.returnCode(), auditRow.errorMessage(),
                java.sql.Date.valueOf(auditRow.executionDate()), auditRow.createdBy(), Timestamp.from(auditRow.createdTs()));
    }

    public static void writePipelineRecordAudit(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, PIPELINE_RECORD_AUDIT_SCHEMA);
        writeAppend(dataset, config.bqOutputRecordAudit());
    }

    // ── shared write path ────────────────────────────────────────────────────

    private static void writeAppend(Dataset<Row> dataset, String fullyQualifiedTable) {
        dataset.write()
                .format("bigquery")
                .option("table", fullyQualifiedTable)
                .mode(SaveMode.Append)
                .save();
    }
}
