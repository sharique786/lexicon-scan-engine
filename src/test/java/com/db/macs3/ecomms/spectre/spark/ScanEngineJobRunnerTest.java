package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.FeatureDecisionViewReader;
import com.db.macs3.ecomms.spectre.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.config.DataprocConfig;
import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.config.ScanEngineProperties;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the empty-BigQuery-view early return {@code ScanEngineJobRunner.runPipeline} added: a run
 * whose view query matches zero rows must complete SUCCESSfully with
 * {@code input_record_count}/{@code output_record_count} both 0, without reading AVRO, resolving a
 * Hyperscan base path, or touching any output table besides {@code pipeline_stage_audit} (written by
 * {@code run()}, not exercised here — see class Javadoc "Known limitations": nothing in this class
 * runs against real BigQuery/GCS credentials).
 *
 * <p>{@code FeatureDecisionViewReader.readFiltered} is the one static call mocked (via Mockito's
 * inline mock maker, already on this project's test classpath — no live BigQuery connection needed)
 * so a real, local-mode {@code SparkSession} can stand in for an empty view result. {@code runPipeline}
 * and {@code RunStats} are package-private specifically so this test can call/inspect them directly —
 * see their Javadoc in {@code ScanEngineJobRunner}.
 */
@DisplayName("ScanEngineJobRunner: empty BigQuery view")
class ScanEngineJobRunnerTest {

    private static SparkSession spark;
    private static JavaSparkContext javaSparkContext;

    private static final String SAMPLE_JSON = "{\"dataset_details\":[{\"dataset_id\":\"ds1\","
            + "\"dataset_partition_value\":\"2026-09-08\"}],\"feature_partition_value\":\"2026-08-16\","
            + "\"pipeline_exec_id\":\"pipe-1\",\"policy_engine_id\":\"policy-1\",\"process_id\":\"proc-1\","
            + "\"trigger_type\":\"policy-alert-test\",\"config_file_path\":\"gs://bucket/config.yml\"}";

    @BeforeAll
    static void startSpark() {
        System.setProperty("spark.master", "local[2]");
        System.setProperty("spark.ui.enabled", "false");
        spark = new SparkSessionConfig().sparkSession();
        javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
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
        try {
            return new ObjectMapper().readValue(SAMPLE_JSON, RuntimeArgs.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static DataprocConfig dataprocConfig() {
        BqTableConfig bigquery = new BqTableConfig("proj", "dataset", "view", "feature-master",
                "language-feature-dec", "feature-hit-summary", "hit-summary", "hit-restricted",
                "hit-unrestricted", "stage-audit", "record-audit");
        DataprocConfig.EngineConfig engine = new DataprocConfig.EngineConfig("lexicon-scan-engine-test",
                new DataprocConfig.HyperscanGcsConfig("hdb-bucket", "hdb-prefix"),
                new DataprocConfig.MessagesGcsConfig("msg-bucket", "msg-prefix", null),
                bigquery);
        return new DataprocConfig("proj", "region", "cluster", 3600L, new DataprocConfig.SpectreConfig(engine));
    }

    /**
     * An empty {@code Dataset<Row>} with just the {@code message_id} column
     * {@code runPipeline}'s distinct-count check reads — standing in for a BigQuery view query that
     * matched zero rows.
     */
    private static Dataset<Row> emptyViewRows() {
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(BqColumns.View.MESSAGE_ID, DataTypes.StringType, true)
        });
        return spark.createDataFrame(Collections.emptyList(), schema);
    }

    @Test
    @DisplayName("an empty view completes gracefully: input/output_record_count = 0, "
            + "and nothing past the view read (groupByMessageId onward) is ever touched")
    void emptyViewShortCircuitsRunPipeline() {
        RuntimeArgs runtimeArgs = runtimeArgs();
        DataprocConfig dataprocConfig = dataprocConfig();
        BqTableConfig tableConfig = dataprocConfig.bigquery();
        ScanEngineJobRunner runner = new ScanEngineJobRunner(
                new GcsClient(), new ScanEngineProperties(), spark, javaSparkContext);
        ScanEngineJobRunner.RunStats stats = new ScanEngineJobRunner.RunStats();

        try (MockedStatic<FeatureDecisionViewReader> viewReader = Mockito.mockStatic(FeatureDecisionViewReader.class)) {
            viewReader.when(() -> FeatureDecisionViewReader.readFiltered(spark, tableConfig, runtimeArgs))
                    .thenReturn(emptyViewRows());

            runner.runPipeline(spark, runtimeArgs, tableConfig, dataprocConfig, stats);

            // groupByMessageId (and everything after it — Hyperscan resolution, AVRO reads, every
            // output table besides pipeline_stage_audit) only ever runs AFTER the early return this
            // test exercises. Seeing it called here would mean the early return didn't actually
            // short-circuit the pipeline.
            viewReader.verify(() -> FeatureDecisionViewReader.readFiltered(spark, tableConfig, runtimeArgs));
            viewReader.verifyNoMoreInteractions();
        }

        assertThat(stats.inputRecordCount).isEqualTo(0);
        assertThat(stats.outputRecordCount).isEqualTo(0);
    }
}
