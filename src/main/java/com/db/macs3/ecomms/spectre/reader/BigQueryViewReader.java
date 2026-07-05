package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.model.JobConfig;
import com.db.macs3.ecomms.spectre.model.ScanEngineArgs;
import com.google.cloud.bigquery.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Responsible for:
 * <ol>
 *   <li>Creating (or replacing) the BigQuery pre-computed join view.</li>
 *   <li>Reading that view as a Spark {@link Dataset} of {@link FeatureDecisionRow}s.</li>
 * </ol>
 *
 * <h2>View extraction logic</h2>
 * <p>Both source tables are filtered and flattened symmetrically:
 *
 * <h3>language-feature-decision (LHS)</h3>
 * <ul>
 *   <li>UNNEST {@code features} (REPEATED)</li>
 *   <li>{@code features.type = 'lexicon'} → emit one row, name = {@code features.name}</li>
 *   <li>{@code features.type = 'composite'} → UNNEST {@code features.sub_feature}
 *       WHERE {@code sub_feature.type = 'lexicon'} → one row PER qualifying
 *       sub-feature, name = {@code sub_feature.name}; the PARENT's id / type /
 *       name / operator / is_noise_reduction are retained on every emitted row</li>
 *   <li>All other {@code features.type} values (metadata, evaluation, etc.) —
 *       and composites with NO lexicon sub-features — are dropped entirely</li>
 * </ul>
 *
 * <h3>feature-master (RHS)</h3>
 * <ul>
 *   <li>{@code feature_type = 'lexicon'} → {@code feature_definition}</li>
 *   <li>{@code feature_type = 'Composite'} → UNNEST {@code sub_feature} WHERE
 *       {@code sub_feature.type = 'lexicon'} → {@code sub_feature.definition}</li>
 * </ul>
 *
 * <h3>Join</h3>
 * <p>{@code LFD.process_id = FM.policy_engine_id AND <resolved lexicon name matches>}
 */
@Component
public class BigQueryViewReader {

    private static final Logger log = LoggerFactory.getLogger(BigQueryViewReader.class);

    private static final String VIEW_DESCRIPTION =
        "Pre-computed join of language-feature-decision x feature-master, " +
        "flattened to one row per (message, lexicon feature or lexicon " +
        "sub-feature of a composite). Created by Lexicon Scan Engine at job startup.";

    private final BigQuery bigQueryClient;

    public BigQueryViewReader() {
        this.bigQueryClient = BigQueryOptions.getDefaultInstance().getService();
    }

    /** Package-private constructor for unit testing. */
    BigQueryViewReader(BigQuery bigQueryClient) {
        this.bigQueryClient = bigQueryClient;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    public Dataset<FeatureDecisionRow> createViewAndRead(SparkSession spark, JobConfig config,
                                                          ScanEngineArgs args) {
        log.info("Creating BQ view: {}", config.bqViewRef());
        createOrReplaceView(config);
        log.info("BQ view created. Reading as Spark Dataset filtered to process_id={}", args.getProcessId());
        return readView(spark, config, args);
    }

    // ── View creation ─────────────────────────────────────────────────────────

    public void createOrReplaceView(JobConfig config) {
        String viewQuery = buildViewSql(config);
        log.debug("View SQL:\n{}", viewQuery);

        TableId viewId = TableId.of(config.getBqProject(), config.getBqDataset(), config.getViewName());
        ViewDefinition viewDef = ViewDefinition.newBuilder(viewQuery).setUseLegacySql(false).build();
        TableInfo tableInfo = TableInfo.newBuilder(viewId, viewDef)
                                        .setDescription(VIEW_DESCRIPTION)
                                        .build();
        try {
            bigQueryClient.update(tableInfo);
            log.info("BQ view updated: {}", config.bqViewRef());
        } catch (BigQueryException e) {
            if (e.getCode() == 404) {
                bigQueryClient.create(tableInfo);
                log.info("BQ view created: {}", config.bqViewRef());
            } else {
                throw new RuntimeException("Failed to create/update BQ view: " + config.bqViewRef(), e);
            }
        }
    }

    public Dataset<FeatureDecisionRow> readView(SparkSession spark, JobConfig config, ScanEngineArgs args) {
        Dataset<Row> rawDf = spark.read()
                .format("bigquery")
                .option("project", config.getBqProject())
                .option("table",   config.bqViewRef())
                .option("filter",  "process_id = '" + args.getProcessId() + "'")
                .load();

        log.info("BQ view schema: {}", rawDf.schema().treeString());

        return rawDf.map(
                (MapFunction<Row, FeatureDecisionRow>) BigQueryViewReader::rowToFeatureDecision,
                Encoders.bean(FeatureDecisionRow.class)
        );
    }

    // ── SQL construction ──────────────────────────────────────────────────────

    public String buildViewSql(JobConfig config) {
        String lfdTable = config.getInputTables().languageFeatureDecision;
        String fmTable  = config.getInputTables().featureMaster;

        return "CREATE OR REPLACE VIEW `" + config.bqViewRef() + "` AS\n\n" +
               "WITH\n\n" +
               "-- Step 1: Flatten LFD.features[] and expand lexicon sub-features of composites.\n" +
               "-- Direct 'lexicon' features -> one row with lexicon_name = features.name.\n" +
               "-- 'composite' features -> UNNEST sub_feature WHERE type='lexicon' -> one row\n" +
               "-- per qualifying sub-feature; PARENT id/type/name/operator/is_noise_reduction\n" +
               "-- are retained on every emitted row (needed for feature-hit-summary reporting).\n" +
               "lfd_direct_lexicon AS (\n" +
               "    SELECT\n" +
               "        lfd.message_id, lfd.run_date, lfd.process_id, lfd.pipeline_exec_id,\n" +
               "        lfd.sent_date, lfd.message_type,\n" +
               "        feat.id AS feature_id, feat.type AS feature_type, feat.name AS feature_name,\n" +
               "        feat.operator AS feature_operator, feat.is_noise_reduction AS is_noise_reduction_raw,\n" +
               "        feat.name AS lexicon_name, FALSE AS from_composite\n" +
               "    FROM `" + lfdTable + "` lfd,\n" +
               "    UNNEST(lfd.features) AS feat\n" +
               "    WHERE feat.type = 'lexicon'\n" +
               "),\n\n" +
               "lfd_composite_lexicon AS (\n" +
               "    SELECT\n" +
               "        lfd.message_id, lfd.run_date, lfd.process_id, lfd.pipeline_exec_id,\n" +
               "        lfd.sent_date, lfd.message_type,\n" +
               "        feat.id AS feature_id, feat.type AS feature_type, feat.name AS feature_name,\n" +
               "        feat.operator AS feature_operator, feat.is_noise_reduction AS is_noise_reduction_raw,\n" +
               "        sf.name AS lexicon_name, TRUE AS from_composite\n" +
               "    FROM `" + lfdTable + "` lfd,\n" +
               "    UNNEST(lfd.features) AS feat,\n" +
               "    UNNEST(feat.sub_feature) AS sf\n" +
               "    WHERE feat.type = 'composite'\n" +
               "    AND   sf.type = 'lexicon'\n" +
               "),\n\n" +
               "lfd_flat AS (\n" +
               "    SELECT * FROM lfd_direct_lexicon\n" +
               "    UNION ALL\n" +
               "    SELECT * FROM lfd_composite_lexicon\n" +
               "),\n\n" +
               "-- Step 2: Resolve feature_definition from feature-master, mirroring the same\n" +
               "-- direct-vs-composite extraction pattern.\n" +
               "fm_direct_lexicon AS (\n" +
               "    SELECT fm.policy_engine_id, fm.feature_name AS lexicon_name,\n" +
               "           fm.feature_definition AS resolved_feature_definition\n" +
               "    FROM `" + fmTable + "` fm\n" +
               "    WHERE fm.feature_type = 'lexicon'\n" +
               "),\n\n" +
               "fm_composite_lexicon AS (\n" +
               "    SELECT fm.policy_engine_id, sf.name AS lexicon_name,\n" +
               "           sf.definition AS resolved_feature_definition\n" +
               "    FROM `" + fmTable + "` fm,\n" +
               "    UNNEST(fm.sub_feature) AS sf\n" +
               "    WHERE fm.feature_type = 'Composite'\n" +
               "    AND   sf.type = 'lexicon'\n" +
               "),\n\n" +
               "fm_all AS (\n" +
               "    SELECT * FROM fm_direct_lexicon\n" +
               "    UNION ALL\n" +
               "    SELECT * FROM fm_composite_lexicon\n" +
               ")\n\n" +
               "-- Step 3: Final join\n" +
               "SELECT\n" +
               "    lfd.message_id, lfd.run_date, lfd.process_id, lfd.pipeline_exec_id,\n" +
               "    lfd.sent_date, lfd.message_type,\n" +
               "    lfd.feature_id, lfd.feature_type, lfd.feature_name, lfd.feature_operator,\n" +
               "    lfd.is_noise_reduction_raw, lfd.lexicon_name, lfd.from_composite,\n" +
               "    fm.resolved_feature_definition AS fm_feature_definition\n" +
               "FROM lfd_flat lfd\n" +
               "INNER JOIN fm_all fm\n" +
               "    ON  lfd.process_id    = fm.policy_engine_id\n" +
               "    AND lfd.lexicon_name  = fm.lexicon_name\n";
    }

    // ── Row mapping ───────────────────────────────────────────────────────────

    private static FeatureDecisionRow rowToFeatureDecision(Row row) {
        return FeatureDecisionRow.of(
                getStr(row, "message_id"),
                getStr(row, "run_date"),
                getStr(row, "process_id"),
                getStr(row, "pipeline_exec_id"),
                getStr(row, "sent_date"),
                getStr(row, "message_type"),
                getStr(row, "feature_id"),
                getStr(row, "feature_type"),
                getStr(row, "feature_name"),
                getStr(row, "feature_operator"),
                getStr(row, "is_noise_reduction_raw"),
                getStr(row, "lexicon_name"),
                !row.isNullAt(row.fieldIndex("from_composite")) && row.getBoolean(row.fieldIndex("from_composite")),
                getStr(row, "fm_feature_definition")
        );
    }

    private static String getStr(Row row, String fieldName) {
        int idx = row.fieldIndex(fieldName);
        return row.isNullAt(idx) ? null : row.getString(idx);
    }
}
