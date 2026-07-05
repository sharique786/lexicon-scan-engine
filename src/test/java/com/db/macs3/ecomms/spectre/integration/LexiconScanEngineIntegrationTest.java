package com.db.macs3.ecomms.spectre.integration;

import com.db.macs3.ecomms.spectre.engine.HyperscanMatcher;
import com.db.macs3.ecomms.spectre.engine.LexiconScanPartitionFunction;
import com.db.macs3.ecomms.spectre.model.*;
import com.db.macs3.ecomms.spectre.reader.JsonMessageReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test exercising the full Lexicon Scan Engine
 * pipeline logic, with every external dependency (BigQuery, GCS) replaced by
 * a local, cross-platform equivalent:
 *
 * <ul>
 *   <li><b>BigQuery view</b> → {@link MockFeatureDecisionData} constructs the
 *       exact {@code Dataset<FeatureDecisionRow>} shape the real view would
 *       produce, bypassing {@code BigQueryViewReader} entirely.</li>
 *   <li><b>Messages</b> → {@link JsonMessageReader} reads
 *       {@code fixtures/messages/scenario1.json} from the test classpath —
 *       no AVRO tooling or GCS bucket required.</li>
 *   <li><b>Hyperscan .hdb files</b> → {@link HdbTestFixtures} compiles tiny
 *       databases using the REAL Hyperscan native library at test run time,
 *       avoiding any platform-specific static binary fixture files.</li>
 * </ul>
 *
 * <p>This test runs identically on Windows, macOS, Linux, and GitHub Actions
 * CI — it opens a local {@code local[2]} SparkSession and touches no cloud
 * services anywhere in its execution path. It is registered under the
 * {@code *IntegrationTest} naming convention so Maven Failsafe (not Surefire)
 * picks it up, keeping {@code mvn test} fast while {@code mvn verify} runs
 * the full integration suite.
 */
@DisplayName("Lexicon Scan Engine — End-to-End Integration Test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LexiconScanEngineIntegrationTest {

    private static SparkSession spark;
    private static JavaSparkContext jsc;

    @BeforeAll
    static void setUpSpark() {
        spark = SparkSession.builder()
                .appName("lexicon-scan-engine-integration-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) spark.stop();
    }

    @BeforeEach
    void clearHyperscanCache() {
        // Ensure each test starts with a clean executor-level DB cache —
        // local[*] mode runs all "executors" in the same JVM as the driver.
        HyperscanMatcher.clearCache();
    }

    // ── Shared fixture wiring ──────────────────────────────────────────────────

    private HdbTestFixtures.FixtureBundle compileAllTestFeatures() throws Exception {
        return HdbTestFixtures.bundle(
                HdbTestFixtures.compileFeature("lexicon_test_alpha",
                        HdbTestFixtures.term("lexicon_test_alpha::1", "bomb")),
                HdbTestFixtures.compileFeature("lexicon_nr_1",
                        HdbTestFixtures.term("lexicon_nr_1::1", "explosive")),
                HdbTestFixtures.compileFeature("lexicon_nr_2",
                        HdbTestFixtures.term("lexicon_nr_2::1", "urgent")),
                HdbTestFixtures.compileFeature("lexicon_test_beta",
                        HdbTestFixtures.term("lexicon_test_beta::1", "market conditions"))
        );
    }

    /**
     * Runs the core scan logic (mirroring {@code LexiconScanEngine.run()}'s
     * phases 3-6) for the given mocked feature-decision rows, returning the
     * resulting {@link MessageScanResult}s keyed by message_id.
     */
    private Map<String, MessageScanResult> runScan(List<FeatureDecisionRow> mockedFeatureDecisions) throws Exception {
        HdbTestFixtures.FixtureBundle fixtures = compileAllTestFeatures();
        Broadcast<Map<String, byte[]>> broadcastHdb = jsc.broadcast(fixtures.hdbBytesByFeature);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> broadcastManifests =
                jsc.broadcast(fixtures.manifestsByFeature);

        Set<String> messageIds = mockedFeatureDecisions.stream()
                .map(FeatureDecisionRow::getMessageId).collect(Collectors.toSet());
        Broadcast<Set<String>> broadcastMsgIds = jsc.broadcast(messageIds);

        // Messages: real JsonMessageReader against the classpath fixture file
        JsonMessageReader messageReader = new JsonMessageReader();
        Dataset<Row> messageDS = messageReader
                .readAndFilter(spark, "classpath:fixtures/messages/scenario1.json", broadcastMsgIds)
                .toDF();

        // Feature decisions: mocked "view output" as a Spark Dataset
        Dataset<FeatureDecisionRow> featureDecisionDS =
                spark.createDataset(mockedFeatureDecisions, Encoders.bean(FeatureDecisionRow.class));

        Dataset<Row> groupedFeatures = featureDecisionDS.toDF()
                .groupBy(col("messageId"))
                .agg(collect_list(struct(
                        col("messageId").alias("message_id"),
                        col("runDate").alias("run_date"),
                        col("processId").alias("process_id"),
                        col("pipelineExecId").alias("pipeline_exec_id"),
                        col("sentDate").alias("sent_date"),
                        col("messageType").alias("message_type"),
                        col("featureId").alias("feature_id"),
                        col("featureType").alias("feature_type"),
                        col("featureName").alias("feature_name"),
                        col("featureOperator").alias("feature_operator"),
                        col("isNoiseReductionRaw").alias("is_noise_reduction_raw"),
                        col("lexiconName").alias("lexicon_name"),
                        col("fromComposite").alias("from_composite"),
                        col("fmFeatureDefinition").alias("fm_feature_definition")
                )).alias("features"))
                .withColumnRenamed("messageId", "msg_id_feat");

        Dataset<Row> joined = messageDS
                .join(groupedFeatures, messageDS.col("message_id").equalTo(groupedFeatures.col("msg_id_feat")), "inner")
                .drop("msg_id_feat");

        Dataset<MessageScanResult> results = joined.mapPartitions(
                new LexiconScanPartitionFunction(broadcastHdb, broadcastManifests, "test-pipeline-1"),
                Encoders.bean(MessageScanResult.class)
        );

        Map<String, MessageScanResult> byMessageId = new HashMap<>();
        for (MessageScanResult r : results.collectAsList()) {
            byMessageId.put(r.getLexiconHitSummaryRow().getMessageId(), r);
        }
        return byMessageId;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test @Order(1)
    @DisplayName("msg-201: standard-only lexicon feature hits 'bomb' -> one evaluated_lexicon, hit_status=Yes")
    void standardFeatureHit() throws Exception {
        Map<String, MessageScanResult> results = runScan(MockFeatureDecisionData.forMsg201());
        MessageScanResult result = results.get("msg-201");

        assertThat(result).isNotNull();
        LexiconHitSummaryRow summary = result.getLexiconHitSummaryRow();
        assertThat(summary.getMessageType()).isEqualTo("unrestricted");
        assertThat(summary.getEvaluatedLexicons()).hasSize(1);

        EvaluatedLexicon lex = summary.getEvaluatedLexicons().get(0);
        assertThat(lex.getName()).isEqualTo("lexicon_test_alpha");
        assertThat(lex.getId()).isEqualTo("201");
        assertThat(lex.getRegexHitCount()).isEqualTo(1);
        assertThat(lex.getTermDtls()).hasSize(1);
        assertThat(lex.getTermDtls().get(0).getTermId()).isEqualTo("lexicon_test_alpha::1");
        assertThat(lex.getTermDtls().get(0).getMatchedText()).contains("\"msg\"").contains("bomb");

        // feature-hit-summary: one row, hit_status=Yes
        assertThat(result.getFeatureHitSummaryRows()).hasSize(1);
        assertThat(result.getFeatureHitSummaryRows().get(0).getHitStatus()).isEqualTo(FeatureHitSummaryRow.HIT_YES);

        // Unrestricted message must NOT produce a restricted-table row
        assertThat(result.hasRestrictedRow()).isFalse();
    }

    @Test @Order(2)
    @DisplayName("msg-202: NR OR composite matches -> standard feature is SKIPPED entirely")
    void noiseReductionSkipsStandardFeature() throws Exception {
        Map<String, MessageScanResult> results = runScan(MockFeatureDecisionData.forMsg202());
        MessageScanResult result = results.get("msg-202");

        assertThat(result).isNotNull();
        LexiconHitSummaryRow summary = result.getLexiconHitSummaryRow();

        // Only the NR composite's matching sub-feature (lexicon_nr_2) should appear —
        // lexicon_test_beta must be ABSENT even though its pattern also matches the text.
        List<String> hitNames = summary.getEvaluatedLexicons().stream()
                .map(EvaluatedLexicon::getName).collect(Collectors.toList());
        assertThat(hitNames).containsExactly("lexicon_nr_2");
        assertThat(hitNames).doesNotContain("lexicon_test_beta");

        // feature-hit-summary must show BOTH NR sub-features evaluated (one Yes, one No),
        // but must NOT contain a row for lexicon_test_beta since it was never scanned.
        List<String> evaluatedSubFeatureNames = result.getFeatureHitSummaryRows().stream()
                .map(FeatureHitSummaryRow::getSubFeatureName).collect(Collectors.toList());
        assertThat(evaluatedSubFeatureNames).containsExactlyInAnyOrder("lexicon_nr_1", "lexicon_nr_2");

        boolean betaEvaluated = result.getFeatureHitSummaryRows().stream()
                .anyMatch(r -> "lexicon_test_beta".equals(r.getFeatureName()));
        assertThat(betaEvaluated).isFalse();
    }

    @Test @Order(3)
    @DisplayName("msg-203: restricted message with hits -> summary redacted, restricted table has real text")
    void restrictedMessageMasking() throws Exception {
        Map<String, MessageScanResult> results = runScan(MockFeatureDecisionData.forMsg203());
        MessageScanResult result = results.get("msg-203");

        assertThat(result).isNotNull();
        LexiconHitSummaryRow summary = result.getLexiconHitSummaryRow();
        assertThat(summary.getMessageType()).isEqualTo("restricted");
        assertThat(summary.getEvaluatedLexicons()).hasSize(1);

        // lexicon-hit-summary must show the REDACTED placeholder, not real text
        String redactedText = summary.getEvaluatedLexicons().get(0).getTermDtls().get(0).getMatchedText();
        assertThat(redactedText).isEqualTo(TermHitDetail.RESTRICTED_PLACEHOLDER);

        // lexicon-hit-restricted MUST be present with the REAL match JSON
        assertThat(result.hasRestrictedRow()).isTrue();
        LexiconHitRestrictedRow restricted = result.getLexiconHitRestrictedRow();
        assertThat(restricted.getMessageId()).isEqualTo("msg-203");
        assertThat(restricted.getEvaluatedLexicons()).hasSize(1);

        String realText = restricted.getEvaluatedLexicons().get(0).getTermDtls().get(0).getMatchedText();
        assertThat(realText).doesNotContain(TermHitDetail.RESTRICTED_PLACEHOLDER);
        // Multi-segment check: msg-203 has "bomb" in BOTH the body and the attachment
        assertThat(realText).contains("\"msg\"").contains("\"attachment-0\"");
    }

    @Test @Order(4)
    @DisplayName("msg-204: zero hits -> evaluated_lexicons empty, feature-hit-summary shows hit_status=No")
    void zeroHitsOmittedFromEvaluatedLexicons() throws Exception {
        Map<String, MessageScanResult> results = runScan(MockFeatureDecisionData.forMsg204());
        MessageScanResult result = results.get("msg-204");

        assertThat(result).isNotNull();
        assertThat(result.getLexiconHitSummaryRow().getEvaluatedLexicons()).isEmpty();

        assertThat(result.getFeatureHitSummaryRows()).hasSize(1);
        assertThat(result.getFeatureHitSummaryRows().get(0).getHitStatus()).isEqualTo(FeatureHitSummaryRow.HIT_NO);

        assertThat(result.hasRestrictedRow()).isFalse();
    }

    @Test @Order(5)
    @DisplayName("Full scenario batch: all 4 messages processed correctly in a single Spark job")
    void fullBatchAllScenarios() throws Exception {
        Map<String, MessageScanResult> results = runScan(MockFeatureDecisionData.allScenarios());

        assertThat(results).containsKeys("msg-201", "msg-202", "msg-203", "msg-204");
        assertThat(results).hasSize(4);

        // Cross-check: exactly one restricted-table row across the whole batch (msg-203 only)
        long restrictedCount = results.values().stream().filter(MessageScanResult::hasRestrictedRow).count();
        assertThat(restrictedCount).isEqualTo(1);
    }
}
