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
            DataTypes.createStructField(BqColumns.LexiconHitSummary.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_SUMMARY_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitSummary.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitSummaryRow summaryRow) {
        List<Row> lexicons = summaryRow.getEvaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.getId(), lexicon.getName(), lexicon.getTotalTermsCount(), lexicon.getRegexHitCount(),
                toNestedArrayValue(lexicon.getTermDtls().stream().map(termDtl -> RowFactory.create(
                        termDtl.getTermId(), termDtl.getTermRegexPattern(), termDtl.getRegexMatchHitCount()))
                        .collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.getMessageId(), summaryRow.getProcessId(), summaryRow.getPipelineExecId(),
                java.sql.Date.valueOf(summaryRow.getDatasetPartitionValue()),
                lexicons, summaryRow.getCreatedBy(), Timestamp.from(summaryRow.getCreatedTs()));
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
            DataTypes.createStructField(BqColumns.LexiconHitDetail.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.EVALUATED_LEXICONS,
                    DataTypes.createArrayType(EVALUATED_LEXICON_DETAIL_TYPE), false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.LexiconHitDetail.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(LexiconHitDetailRow detailRow) {
        List<Row> lexicons = detailRow.getEvaluatedLexicons().stream().map(lexicon -> RowFactory.create(
                lexicon.getId(), toNestedArrayValue(lexicon.getTermDtls().stream().map(termDtl -> RowFactory.create(
                        termDtl.getTermId(), termDtl.getMatchedText())).collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(detailRow.getMessageId(), detailRow.getProcessId(), detailRow.getPipelineExecId(),
                java.sql.Date.valueOf(detailRow.getDatasetPartitionValue()),
                lexicons, detailRow.getCreatedBy(), Timestamp.from(detailRow.getCreatedTs()));
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
            DataTypes.createStructField(BqColumns.FeatureHitSummary.DATASET_PARTITION_VALUE, DataTypes.DateType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PIPELINE_EXEC_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.PROCESS_ID, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURE_HIT_TYPE, DataTypes.StringType, true),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.FEATURES, DataTypes.createArrayType(FEATURE_TYPE), false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_BY, DataTypes.StringType, false),
            DataTypes.createStructField(BqColumns.FeatureHitSummary.CREATED_TS, DataTypes.TimestampType, false),
    });

    public static Row toRow(FeatureHitSummaryRow summaryRow) {
        List<Row> features = summaryRow.getFeatures().stream().map(feature -> RowFactory.create(
                feature.getId(), feature.getName(), feature.getType(), feature.isNoiseReduction(), feature.isHitStatus(),
                toNestedArrayValue(feature.getSubFeatures().stream().map(subFeature -> RowFactory.create(
                        subFeature.getType(), subFeature.getName(), subFeature.isHitStatus())).collect(Collectors.toList()))
        )).collect(Collectors.toList());
        return RowFactory.create(summaryRow.getMessageId(), java.sql.Date.valueOf(summaryRow.getDatasetPartitionValue()),
                summaryRow.getPipelineExecId(), summaryRow.getProcessId(), summaryRow.getFeatureHitType(), features,
                summaryRow.getCreatedBy(), Timestamp.from(summaryRow.getCreatedTs()));
    }

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

    public static void writePipelineStageAudit(SparkSession spark, BqTableConfig config, PipelineStageAuditRow row) {
        // Single-row audit write — a normal driver-side write of ONE small row is not a
        // scaling concern (contrast the per-message tables above, always written from an
        // already-distributed Dataset).
        Dataset<Row> dataset = spark.createDataFrame(List.of(toRow(row)), PIPELINE_STAGE_AUDIT_SCHEMA);
        writeAppend(dataset, config.bqOutputStageAudit());
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
