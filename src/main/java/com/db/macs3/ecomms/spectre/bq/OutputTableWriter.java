package com.db.macs3.ecomms.spectre.bq;

import com.db.macs3.ecomms.spectre.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.output.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts each of the 6 output/audit row records into a Spark
 * {@code Dataset<Row>} against an explicit {@link StructType} schema, then
 * writes it to BigQuery via the Spark BigQuery connector
 * ({@code write().format("bigquery")}, append mode — every write here adds
 * new rows; this job never updates rows in place).
 *
 * <p>Two write methods are used, split by table scale (see
 * {@code writeAppend}/{@code writeAppendDirect} below): the four
 * message-scale tables (millions of rows) use {@code writeMethod=indirect}
 * (the connector default — stage to GCS, then one BigQuery load job) with
 * {@code intermediateFormat=avro}; the two small audit tables use
 * {@code writeMethod=direct} (Storage Write API, no GCS staging) for lower
 * per-write latency, since their volume is negligible.
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

    private static final Logger log = LoggerFactory.getLogger(OutputTableWriter.class);

    private OutputTableWriter() {}

    /**
     * Converts a {@code List<Row>} bound for an ARRAY field NESTED inside a
     * struct that is itself an array element (e.g. {@code evaluated_lexicons[].term_dtls[]},
     * {@code features[].sub_features[]}) to a genuine {@code scala.collection.Seq}.
     *
     * <p>{@code spark.createDataFrame(JavaRDD<Row>, StructType)}'s generated
     * row serializer only tolerates a plain {@code java.util.List} for an
     * ARRAY field at the TOP level of the outer row — for an ARRAY field
     * nested one level deeper (inside a struct that is itself inside an
     * array), the codegen'd {@code MapObjects} expects
     * {@code scala.collection.Seq} and throws {@code ClassCastException:
     * class java.util.ArrayList cannot be cast to class scala.collection.Seq}
     * if handed a plain {@code java.util.List} instead — confirmed via a real
     * production stack trace. Every OUTER array field below (e.g.
     * {@code evaluated_lexicons}, {@code features}) stays a plain
     * {@code List<Row>}; only the INNER one needs this conversion.
     */
    private static scala.collection.Seq<Row> toNestedArrayValue(List<Row> rows) {
        return scala.jdk.CollectionConverters.ListHasAsScala(rows).asScala().toSeq();
    }

    // ── lexicon-hit-summary ──────────────────────────────────────────────────

    private static final StructType TERM_DTL_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.TERM_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.TERM_REGEX_PATTERN, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.TermDtl.REGEX_MATCH_HIT_COUNT, DataTypes.LongType, true),
    });
    private static final StructType EVALUATED_LEXICON_SUMMARY_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.TOTAL_TERMS_COUNT, DataTypes.LongType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.REGEX_HIT_COUNT, DataTypes.LongType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EvaluatedLexicon.TERM_DTLS,
                    DataTypes.createArrayType(TERM_DTL_SUMMARY_TYPE), false),
    });
    // Field order and NOT NULL/NULLABLE mode rechecked against the live BigQuery table —
    // evaluated_lexicons precedes dataset_partition_value; total_terms_count/regex_hit_count/
    // created_by are NULLABLE (not REQUIRED as an earlier revision of this schema had them).
    public static final StructType LEXICON_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitSummary.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_SUMMARY_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_BY, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static void writeLexiconHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, LEXICON_HIT_SUMMARY_SCHEMA);
        writeAppend(dataset, config.bqOutputHitSummary());
    }

    // ── lexicon-hit-restricted / lexicon-hit-unrestricted (shared schema) ───

    private static final StructType TERM_DTL_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.TermDtl.TERM_ID, DataTypes.StringType, false),
            // BigQuery's declared type is JSON; Spark has no first-class JSON type, so this carries
            // the same value as a StringType holding valid JSON text — BigQuery coerces it into the
            // destination JSON column on write. NULLABLE per the delivered schema (was REQUIRED).
            DataTypes.createStructField(BqColumns.LexiconHitDetail.TermDtl.MATCHED_TEXT, DataTypes.StringType, true),
    });
    private static final StructType EVALUATED_LEXICON_DETAIL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EvaluatedLexicon.ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EvaluatedLexicon.TERM_DTLS,
                    DataTypes.createArrayType(TERM_DTL_DETAIL_TYPE), false),
    });
    // Field order rechecked against the live BigQuery table (both restricted and unrestricted —
    // identical shape): evaluated_lexicons precedes dataset_partition_value.
    public static final StructType LEXICON_HIT_DETAIL_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.LexiconHitDetail.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_DETAIL_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_BY, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static void writeLexiconHitDetail(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows, boolean restricted) {
        Dataset<Row> dataset = spark.createDataFrame(rows, LEXICON_HIT_DETAIL_SCHEMA);
        String fullyQualifiedTable = restricted ? config.bqOutputHitRestricted() : config.bqOutputHitUnrestricted();
        writeAppend(dataset, fullyQualifiedTable);
    }

    // ── feature-hit-summary ──────────────────────────────────────────────────

    private static final StructType SUB_FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.SubFeature.HIT_STATUS, DataTypes.BooleanType, true),
    });
    private static final StructType FEATURE_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.ID, DataTypes.LongType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.IS_NOISE_REDUCTION, DataTypes.BooleanType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.HIT_STATUS, DataTypes.BooleanType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.Feature.SUB_FEATURES,
                    DataTypes.createArrayType(SUB_FEATURE_TYPE), false),
    });
    // Field order and NOT NULL/NULLABLE mode rechecked against the live BigQuery table — notably
    // different from the other two tables above: features precedes dataset_partition_value, and
    // process_id/pipeline_exec_id come LAST, after created_by/created_ts. created_by/created_ts
    // are NULLABLE here (unlike lexicon-hit-summary/-detail, where created_ts is REQUIRED). Do not
    // "fix" this ordering to match the other tables — see FeatureHitSummaryRow class Javadoc.
    public static final StructType FEATURE_HIT_SUMMARY_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.FeatureHitSummary.MESSAGE_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURES, DataTypes.createArrayType(FEATURE_TYPE), false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURE_HIT_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_BY, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_TS, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PIPELINE_EXEC_ID, DataTypes.StringType, false),
    });

    public static void writeFeatureHitSummary(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, FEATURE_HIT_SUMMARY_SCHEMA);
        writeAppend(dataset, config.bqOutputFeatureHitSummary());
    }

    // ── pipeline_stage_audit ─────────────────────────────────────────────────

    private static final StructType MODEL_CONFIG_DTLS_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ModelConfigDtls.MODEL_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ModelConfigDtls.TEMPRATURE, DataTypes.FloatType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ModelConfigDtls.TOP_P, DataTypes.FloatType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ModelConfigDtls.THINKING_BUDGET, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ModelConfigDtls.MAX_OUTPUT_TOKEN, DataTypes.IntegerType, true),
    });

    public static final StructType PIPELINE_STAGE_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineStageAudit.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.TRIGGER_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.EVAL_TEST_ID, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.STAGE_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.COMPOSER_DAG_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.COMPOSER_DAG_PATH, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.DPROC_SCRIPT_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.DPROC_SCRIPT_PATH, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.MODEL_CONFIG_DTLS, MODEL_CONFIG_DTLS_TYPE, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.START_TIME, DataTypes.TimestampType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.END_TIME, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.JOB_STATUS, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.INPUT_FILE_COUNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.OUTPUT_FILE_COUNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.INPUT_RECORD_COUNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.OUTPUT_RECORD_COUNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ERROR_COUNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ERROR_MESSAGE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.ADDITIONAL_INFO, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.LOG_PATH, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.EXECUTION_DATE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.RERUN_FLG, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.RERUN_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineStageAudit.RERUN_PROCESS_ID, DataTypes.StringType, true),
    });

    public static void writePipelineStageAudit(SparkSession spark, BqTableConfig config, PipelineStageAuditRow row) {
        // Single-row audit write — a normal driver-side write of ONE small row is not a
        // scaling concern (contrast the per-message tables above, always written from an
        // already-distributed Dataset).
        Dataset<Row> dataset = spark.createDataFrame(List.of(toRow(row)), PIPELINE_STAGE_AUDIT_SCHEMA);
        writeAppendDirect(dataset, config.bqOutputStageAudit());
    }

    // ── pipeline_record_audit ────────────────────────────────────────────────

    private static final StructType RULE_DTL_TYPE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RuleDtl.RULE_ID, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RuleDtl.RULE_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RuleDtl.RULE_VERSION, DataTypes.IntegerType, true),
    });

    public static final StructType PIPELINE_RECORD_AUDIT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.TRIGGER_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.EVAL_TEST_ID, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RECORD_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.STAGE_NAME, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_INPUT_FILE_NM, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_INPUT_FILE_PATH, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_OUTPUT_FILE_PATH, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_OUTPUT_FILE_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_OUTPUT_FILE_NM, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.STATUS, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RETURN_CODE, DataTypes.IntegerType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.ERROR_MESSAGE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.EVALUATED_RULES_DTLS,
                    DataTypes.createArrayType(RULE_DTL_TYPE), true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.EVALUATED_RULES_CNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.DETECTED_RULES_DTLS,
                    DataTypes.createArrayType(RULE_DTL_TYPE), true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.DETECTED_RULES_CNT, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.SYS_PROMPT_EVAL_RULES_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.INPUT_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.OUTPUT_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.THINKING_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.CACHED_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.MSG_ATTACH_TEXT_TOKENS, DataTypes.IntegerType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.CREATED_TS, DataTypes.TimestampType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.ADDITION_INFO, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.EXECUTION_DATE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.SENT_DATE, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RUN_DATE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.SOURCE_NAME, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.GEMINI_REQUEST_START_TIME, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.GEMINI_REQUEST_END_TIME, DataTypes.TimestampType, true),
            DataTypes.createStructField(BqColumns.PipelineRecordAudit.RERUN_PROCESS_ID, DataTypes.StringType, true),
    });

    private static List<Row> toRuleDtlRows(List<RuleDtlType> ruleDtls) {
        if (ruleDtls == null) {
            return null;
        }
        // Row order follows the SCHEMA's struct field order (rule_id, rule_name, rule_version),
        // which does not match RuleDtlType's own constructor-argument order — see that class's Javadoc.
        return ruleDtls.stream()
                .map(ruleDtl -> RowFactory.create(ruleDtl.getRuleId(), ruleDtl.getRuleName(), ruleDtl.getRuleVersion()))
                .collect(Collectors.toList());
    }

    public static Row toRow(LexiconHitSummaryRow summaryRow) {
        List<Row> lexicons = summaryRow.getEvaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.getId(), lexicon.getName(), lexicon.getTotalTermsCount(), lexicon.getRegexHitCount(),
                toNestedArrayValue(lexicon.getTermDtls().stream().map(termDtl -> RowFactory.create(
                                termDtl.getTermId(), termDtl.getTermRegexPattern(), termDtl.getRegexMatchHitCount()))
                        .collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.getMessageId(), summaryRow.getProcessId(), summaryRow.getPipelineExecId(),
                lexicons, java.sql.Date.valueOf(summaryRow.getDatasetPartitionValue()),
                summaryRow.getCreatedBy(), Timestamp.from(summaryRow.getCreatedTs()));
    }

    public static Row toRow(LexiconHitDetailRow detailRow) {
        List<Row> lexicons = detailRow.getEvaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.getId(), toNestedArrayValue(lexicon.getTermDtls().stream().map(termDtl -> RowFactory.create(
                        termDtl.getTermId(), termDtl.getMatchedText())).collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(detailRow.getMessageId(), detailRow.getProcessId(), detailRow.getPipelineExecId(),
                lexicons, java.sql.Date.valueOf(detailRow.getDatasetPartitionValue()),
                detailRow.getCreatedBy(), Timestamp.from(detailRow.getCreatedTs()));
    }

    public static Row toRow(FeatureHitSummaryRow summaryRow) {
        List<Row> features = summaryRow.getFeatures().stream().map(feature -> RowFactory.create(
                feature.getId(), feature.getName(), feature.getType(), feature.getIsNoiseReduction(), feature.getHitStatus(),
                toNestedArrayValue(feature.getSubFeatures().stream().map(subFeature -> RowFactory.create(
                        subFeature.getType(), subFeature.getName(), subFeature.getHitStatus())).collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.getMessageId(), features,
                java.sql.Date.valueOf(summaryRow.getDatasetPartitionValue()), summaryRow.getFeatureHitType(),
                summaryRow.getCreatedBy(),
                summaryRow.getCreatedTs() == null ? null : Timestamp.from(summaryRow.getCreatedTs()),
                summaryRow.getProcessId(), summaryRow.getPipelineExecId());
    }

    public static Row toRow(PipelineStageAuditRow auditRow) {
        ModelConfigDtls modelConfig = auditRow.getModelConfigDtls();
        Row modelConfigRow = modelConfig == null ? null : RowFactory.create(
                modelConfig.getModelName(), modelConfig.getTemprature(), modelConfig.getTopP(),
                modelConfig.getThinkingBudget(), modelConfig.getMaxOutputToken());
        return RowFactory.create(auditRow.getProcessId(), auditRow.getTriggerType(), auditRow.getEvalTestId(),
                auditRow.getPipelineExecId(), auditRow.getStageName(), auditRow.getComposerDagName(), auditRow.getComposerDagPath(),
                auditRow.getDprocScriptName(), auditRow.getDprocScriptPath(), modelConfigRow,
                auditRow.getStartTime() == null ? null : Timestamp.from(auditRow.getStartTime()),
                auditRow.getEndTime() == null ? null : Timestamp.from(auditRow.getEndTime()),
                auditRow.getJobStatus(), auditRow.getInputFileCount(), auditRow.getOutputFileCount(),
                auditRow.getInputRecordCount(), auditRow.getOutputRecordCount(), auditRow.getErrorCount(),
                auditRow.getErrorMessage(), auditRow.getAdditionalInfo(), auditRow.getLogPath(),
                java.sql.Date.valueOf(auditRow.getExecutionDate()), auditRow.getRerunFlg(), auditRow.getRerunType(),
                auditRow.getRerunProcessId());
    }

    public static Row toRow(PipelineRecordAuditRow auditRow) {
        return RowFactory.create(auditRow.getProcessId(), auditRow.getTriggerType(), auditRow.getEvalTestId(),
                auditRow.getPipelineExecId(), auditRow.getRecordId(), auditRow.getStageName(), auditRow.getMsgInputFileNm(),
                auditRow.getMsgInputFilePath(), auditRow.getMsgOutputFilePath(), auditRow.getMsgOutputFileType(),
                auditRow.getMsgOutputFileNm(), auditRow.getStatus(), auditRow.getReturnCode(), auditRow.getErrorMessage(),
                toRuleDtlRows(auditRow.getEvaluatedRulesDtls()), auditRow.getEvaluatedRulesCnt(),
                toRuleDtlRows(auditRow.getDetectedRulesDtls()), auditRow.getDetectedRulesCnt(),
                auditRow.getSysPromptEvalRulesTokens(), auditRow.getInputTokens(), auditRow.getOutputTokens(),
                auditRow.getThinkingTokens(), auditRow.getCachedTokens(), auditRow.getMsgMatchTextTokens(),
                Timestamp.from(auditRow.getCreatedTs()), auditRow.getCreatedBy(), auditRow.getAdditionInfo(),
                java.sql.Date.valueOf(auditRow.getExecutionDate()),
                auditRow.getSentDate() == null ? null : Timestamp.from(auditRow.getSentDate()),
                auditRow.getRunDate(), auditRow.getSourceName(),
                auditRow.getGeminiRequestStartTime() == null ? null : Timestamp.from(auditRow.getGeminiRequestStartTime()),
                auditRow.getGeminiRequestEndTime() == null ? null : Timestamp.from(auditRow.getGeminiRequestEndTime()),
                auditRow.getRerunProcessId());
    }

    public static void writePipelineRecordAudit(SparkSession spark, BqTableConfig config, JavaRDD<Row> rows) {
        Dataset<Row> dataset = spark.createDataFrame(rows, PIPELINE_RECORD_AUDIT_SCHEMA);
        writeAppendDirect(dataset, config.bqOutputRecordAudit());
    }

    // ── shared write path ────────────────────────────────────────────────────

    // intermediateFormat=avro: the connector's indirect write defaults to Parquet, whose
    // 3-level LIST encoding for a REPEATED RECORD (evaluated_lexicons[]/features[]) sitting
    // alongside its own nested REPEATED sub-field (term_dtls[]/sub_features[]) is a documented
    // spark-bigquery-connector defect — the BQ load job rejects it with "Schema mismatch:
    // referenced variable '...id' has array levels of 1, while the corresponding field path to
    // Parquet column has 0 repeated fields" even though the destination table schema and this
    // job's Spark schema agree exactly (confirmed against the live lexicon-hit-summary table).
    // Avro's array representation doesn't hit this bug. See GoogleCloudDataproc/
    // spark-bigquery-connector issues #474 and #1466.
    //
    // Used for the four message-scale tables (lexicon-hit-summary/-restricted/-unrestricted,
    // feature-hit-summary) — at "millions of messages" scale, indirect's single batch load job
    // outperforms the direct Storage Write API's per-row/per-batch overhead. See
    // writeAppendDirect below for the two small audit tables, where the opposite tradeoff wins.
    //
    // useAvroLogicalTypes=true: without it, the BQ load job ignores the Avro logicalType
    // annotation Spark attaches to DATE/TIMESTAMP columns (dataset_partition_value/created_ts
    // here) and reads the underlying physical Avro type literally — failing with "Field
    // dataset_partition_value has incompatible types. Configured schema: date; Avro file:
    // integer." even though the actual data is correct. Defaults to false; only meaningful
    // alongside intermediateFormat=avro. See GoogleCloudDataproc/spark-bigquery-connector
    // issues #300 and #612, and the connector's own README ("useAvroLogicalTypes").
    private static void writeAppend(Dataset<Row> dataset, String fullyQualifiedTable) {
        log.info("Starting indirect (avro) write to BigQuery table {}", fullyQualifiedTable);
        Instant writeStart = Instant.now();
        boolean succeeded = false;
        try {
            dataset.write()
                    .format("bigquery")
                    .option("table", fullyQualifiedTable)
                    .option("intermediateFormat", "avro")
                    .option("useAvroLogicalTypes", "true")
                    .mode(SaveMode.Append)
                    .save();
            succeeded = true;
        } catch (RuntimeException e) {
            // Not swallowed — a write failure here must fail the whole job (see
            // ScanEngineJobRunner#run, which records it to pipeline_stage_audit). Logged with
            // the target table before rethrow since the connector's own exception message
            // often does not name it (see the schema-mismatch investigations this table's
            // write path has been through — array/date-logical-type issues above).
            log.error("Indirect write to BigQuery table {} failed: {}", fullyQualifiedTable, e.getMessage(), e);
            throw e;
        } finally {
            // finally, not just a post-save() log line, so the elapsed time is captured even
            // when save() throws — that duration (e.g. "failed after 40 minutes" vs. "after 5
            // seconds") is itself useful triage information the catch block alone can't give.
            long elapsedMs = Duration.between(writeStart, Instant.now()).toMillis();
            log.info("Indirect write to BigQuery table {} {} in {}ms",
                    fullyQualifiedTable, succeeded ? "completed" : "failed", elapsedMs);
        }
    }

    // writeMethod=direct: no intermediate GCS file/load job — rows go straight to BigQuery via
    // the Storage Write API, with schema compatibility checked up front in-process rather than
    // inside a load job. Lower per-write latency, no GCS staging round-trip; used only for the
    // two tiny audit tables (pipeline_stage_audit: 2 rows/run; pipeline_record_audit: failed
    // messages only) where that latency actually matters and the overhead is negligible — see
    // README's write-method guidance. Do not use this for the message-scale tables above.
    private static void writeAppendDirect(Dataset<Row> dataset, String fullyQualifiedTable) {
        log.info("Starting direct write to BigQuery table {}", fullyQualifiedTable);
        Instant writeStart = Instant.now();
        boolean succeeded = false;
        try {
            dataset.write()
                    .format("bigquery")
                    .option("table", fullyQualifiedTable)
                    .option("writeMethod", "direct")
                    .mode(SaveMode.Append)
                    .save();
            succeeded = true;
        } catch (RuntimeException e) {
            // Direct write's schema check runs up front (see the process_Id casing
            // investigation) — this is where a column-name/case/type drift between this
            // job's Spark schema and the live table surfaces. Not swallowed, same reasoning
            // as writeAppend above.
            log.error("Direct write to BigQuery table {} failed: {}", fullyQualifiedTable, e.getMessage(), e);
            throw e;
        } finally {
            long elapsedMs = Duration.between(writeStart, Instant.now()).toMillis();
            log.info("Direct write to BigQuery table {} {} in {}ms",
                    fullyQualifiedTable, succeeded ? "completed" : "failed", elapsedMs);
        }
    }
}
