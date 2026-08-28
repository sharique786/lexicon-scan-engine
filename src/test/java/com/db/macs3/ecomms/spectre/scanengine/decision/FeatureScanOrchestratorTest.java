package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanDatabaseLoader;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermMetadataLoader;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.message.*;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FeatureScanOrchestrator}.
 *
 * <h2>Confirmed gap this test class covers: AND NOT term_id / hit evaluation</h2>
 * <p>An earlier version of this class (and {@code FeatureScanOrchestrator})
 * assumed every PASS term's reportable Hyperscan expression id was always
 * its own term number, whether or not it needed AND NOT. Confirmed BROKEN
 * once the Compile Service's AND NOT fix removed native COMBINATION for AND
 * NOT terms — see {@link TermExpressionMetadata} class Javadoc (Lexicon
 * Scan Engine project) for the full explanation. The tests below
 * ({@link #andNotTerm_requiredBeforeExcludedInText_noFalsePositive},
 * {@link #andNotTerm_correctTermIdWhenMatched},
 * {@link #decomposedTerm_termRegexPatternIsReadable_notCombinationFormula})
 * directly cover the confirmed, concrete production-impact bugs this fix
 * addresses.
 *
 * <h2>Real Hyperscan, not a stub</h2>
 * <p>{@link #compileAndSerialize} calls the REAL native
 * {@code Database.compile}/{@code Database.save} from the
 * {@code com.gliwka.hyperscan:hyperscan} dependency this project already
 * declares, producing genuine {@code .hdb}-format bytes for
 * {@link HyperscanDatabaseLoader} to load via an in-memory
 * {@link ByteArrayInputStream} — exercising the same native compile/scan
 * path a live Dataproc job would, including real {@code COMBINATION}/
 * {@code QUIET} semantics for the decomposed-term test below. An earlier
 * version of this file depended on non-existent test-only static fields
 * ({@code Database.testPathToExpressions} et al.) on
 * {@code com.gliwka.hyperscan.wrapper.Database} — that class has no such
 * fields in the real dependency (confirmed against the dependency's own
 * sources jar), so this file did not compile against the real Hyperscan
 * library at all until this rewrite.
 */
@DisplayName("FeatureScanOrchestrator")
class FeatureScanOrchestratorTest {

    /**
     * Compiles {@code expressions} into a real Hyperscan {@link Database} and
     * serializes it via {@link Database#save}, in the exact format
     * {@link Database#load} (and therefore {@code HyperscanDatabaseLoader})
     * expects — i.e. genuine {@code .hdb} bytes, not a mock.
     */
    private static byte[] compileAndSerialize(Expression... expressions) {
        try (Database database = Database.compile(List.of(expressions))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            database.save(out);
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile/serialize test Hyperscan database: " + e.getMessage(), e);
        }
    }

    private static HyperscanDatabaseLoader.GcsByteStreamer streamerFor(byte[] dbBytes) {
        return path -> new ByteArrayInputStream(dbBytes);
    }

    private static FeatureDecisionRow row(String featureId, String featuresToApply, String defJson) {
        return new FeatureDecisionRow("proc-1", "msg-101", "part-1", "Lexicon-Tagging",
                "lexicon", featureId, featureId + "-name", null, featuresToApply,
                "N", null, defJson, "2026-08-16", "101");
    }

    private static String defJson(String feature, String... scopes) {
        StringBuilder scopeArr = new StringBuilder("[");
        for (int i = 0; i < scopes.length; i++) {
            if (i > 0) scopeArr.append(",");
            scopeArr.append("\"").append(scopes[i]).append("\"");
        }
        scopeArr.append("]");
        return "{\"featureName\":\"x\",\"featureType\":\"Lexicon\",\"isNoiseReduction\":false,"
                + "\"body\":{\"feature\":\"" + feature + "\",\"totalTermsCount\":5,\"minimumHits\":1,"
                + "\"scope\":" + scopeArr + "}}";
    }

    /**
     * Builds a metadata loader whose {@code feature -> metadata path} map and
     * streamer directly return {@code json} for {@code metadataPath} — mirrors
     * {@link #streamerFor}'s simplicity for the {@code .hdb} side.
     */
    private static TermMetadataLoader metadataLoader(String feature, String metadataPath, String json) {
        return new TermMetadataLoader(
                Map.of(feature, metadataPath),
                path -> new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                10);
    }

    /** A non-AND-NOT term's compile-results JSON entry — {@code hyperscanExpressionId} only. */
    private static String simpleTermJson(String feature, int termNumber, String... translatedPattern) {
        return """
            {"termId": "%s::%d", "compilationStatus": "PASS", "translatedPattern": [%s],
             "requiresExclusionCheck": false, "hyperscanExpressionId": %d}
            """.formatted(feature, termNumber, quotedCsv(translatedPattern), termNumber);
    }

    /** An AND NOT term's compile-results JSON entry — requiredExpressionIds/excludedExpressionIds, no hyperscanExpressionId. */
    private static String andNotTermJson(String feature, int termNumber, List<String> requiredPatterns,
                                          List<Integer> requiredIds, List<String> excludedPatterns, List<Integer> excludedIds) {
        return """
            {"termId": "%s::%d", "compilationStatus": "PASS", "translatedPattern": [%s],
             "requiresExclusionCheck": true, "requiredExpressionIds": %s, "excludedExpressionIds": %s}
            """.formatted(feature, termNumber, quotedCsv(requiredPatterns.toArray(new String[0])),
                    requiredIds, excludedIds);
    }

    private static String quotedCsv(String... values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(values[i]).append("\"");
        }
        return sb.toString();
    }

    private static String wrapResults(String... termEntries) {
        return "{\"results\": [" + String.join(",", termEntries) + "]}";
    }

    // ── Pre-existing behaviour, unaffected by the fix ───────────────────────────

    @Test
    @DisplayName("only scans the areas the feature's scope covers")
    void respectsScopeAreaSelection() {
        String feature = "lex_bomb-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_bomb-1.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_bomb-1-compile-results.json",
                wrapResults(simpleTermJson(feature, 1, "bomb")));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "there is a bomb in the body", "bomb in subject too", null),
                List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in attachment too")),
                new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow bodyOnlyRow = row("1", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(bodyOnlyRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).matches()).hasSize(1);
        assertThat(results.get(0).matches().get(0).area()).isEqualTo(MatchArea.MESSAGE_BODY);
    }

    @Test
    @DisplayName("merges matches for the same term across multiple areas into ONE TermMatchResult")
    void mergesMatchesAcrossAreas() {
        String feature = "lex_bomb-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 7));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_bomb-2.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_bomb-2-compile-results.json",
                wrapResults(simpleTermJson(feature, 7, "bomb")));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "there is a bomb in the body", "bomb in subject too", null),
                List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in attachment too")),
                new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow allScopeRow = row("2", feature, defJson(feature, "subject", "Message Body", "Attachment"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(allScopeRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).matches()).hasSize(3);
        assertThat(results.get(0).termId()).isEqualTo(feature + "::7");

        Set<MatchArea> areas = new HashSet<>();
        for (var m : results.get(0).matches()) areas.add(m.area());
        assertThat(areas).hasSize(3);
    }

    @Test
    @DisplayName("skips an attachment exceeding the configured size limit entirely")
    void skipsOversizedAttachment() {
        String feature = "lex_bomb-3";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_bomb-3.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_bomb-3-compile-results.json",
                wrapResults(simpleTermJson(feature, 1, "bomb")));

        FeatureScanOrchestrator limited = new FeatureScanOrchestrator(dbLoader, metaLoader, 5L); // 5-byte limit

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, null, null, null),
                List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in a long attachment")),
                new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow attachOnlyRow = row("3", feature, defJson(feature, "Attachment"));
        assertThat(limited.scannerFor(message).scan(attachOnlyRow)).isEmpty();

        FeatureScanOrchestrator unlimited = new FeatureScanOrchestrator(dbLoader, metaLoader, null);
        assertThat(unlimited.scannerFor(message).scan(attachOnlyRow)).hasSize(1);
    }

    @Test
    @DisplayName("termRegexPattern is populated from term metadata (the Compile Service's own " +
                 "translatedPattern, not the raw matched Expression text)")
    void termRegexPatternPopulatedFromMetadata() {
        String feature = "lex_bomb-4";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_bomb-4.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_bomb-4-compile-results.json",
                wrapResults(simpleTermJson(feature, 1, "bomb")));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "there is a bomb in the body", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow bodyOnlyRow = row("4", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(bodyOnlyRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).termRegexPattern()).isEqualTo("bomb");
        assertThat(results.get(0).termId()).isEqualTo(feature + "::1");
    }

    @Test
    @DisplayName("The expression id -> termId mapping works uniformly for high, non-index-like term " +
                 "numbers too — the Compile Service's id scheme guarantees a non-AND-NOT PASS term's " +
                 "reportable id is always its own term number, whatever that number is")
    void expressionIdMappingWorksForHighTermNumbers() {
        String feature = "lex_bomb-5";
        byte[] dbBytes = compileAndSerialize(
                new Expression("insider", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 47));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_bomb-5.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_bomb-5-compile-results.json",
                wrapResults(simpleTermJson(feature, 47, "insider")));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "insider trading", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow decisionRow = row("5", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).termId()).isEqualTo(feature + "::47");
    }

    // ── REGRESSION: the confirmed AND NOT gaps this fix addresses ──────────────

    @Test
    @DisplayName("REGRESSION: AND NOT term does NOT produce a false-positive hit when required " +
                 "appears BEFORE excluded in the same message — the exact scenario from the original " +
                 "issue report. Confirmed broken by Hyperscan's own documented eager, progressive " +
                 "combination evaluation before the fix; AND NOT no longer uses native COMBINATION at all now.")
    void andNotTerm_requiredBeforeExcludedInText_noFalsePositive() {
        String feature = "lex_andnot-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("insider", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 5),
                new Expression("disclosed", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 6));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_andnot-1.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_andnot-1-compile-results.json",
                wrapResults(andNotTermJson(feature, 3, List.of("insider"), List.of(5), List.of("disclosed"), List.of(6))));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "insider trading occurred and was later disclosed to the board", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow decisionRow = row("6", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

        assertThat(results)
                .as("required present AND excluded ALSO present -> term must NOT appear in results")
                .isEmpty();
    }

    @Test
    @DisplayName("AND NOT term correctly matches with the CORRECT term_id when excluded is absent " +
                 "(not a raw auxiliary expression id like 5 or 6)")
    void andNotTerm_correctTermIdWhenMatched() {
        String feature = "lex_andnot-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("insider", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 5),
                new Expression("disclosed", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 6));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_andnot-2.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_andnot-2-compile-results.json",
                wrapResults(andNotTermJson(feature, 3, List.of("insider"), List.of(5), List.of("disclosed"), List.of(6))));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "insider trading occurred yesterday", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow decisionRow = row("7", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).termId())
                .as("must be the term's OWN number (3), never a raw auxiliary id like 5 or 6")
                .isEqualTo(feature + "::3");
        assertThat(results.get(0).matches().get(0).span().matchedText()).isEqualTo("insider");
    }

    @Test
    @DisplayName("AND NOT with a decomposed required side: excluded ONLY when EVERY required leaf " +
                 "matched — mirrors the Compile Service's/Scanner Service's own documented AND " +
                 "convention on the required side")
    void andNotTerm_decomposedRequiredSide_allLeavesMustMatch() {
        String feature = "lex_andnot-3";
        byte[] dbBytes = compileAndSerialize(
                new Expression("alpha", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 10),
                new Expression("beta", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 11),
                new Expression("gamma", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 12),
                new Expression("excluded", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 13));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_andnot-3.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_andnot-3-compile-results.json",
                wrapResults(andNotTermJson(feature, 9, List.of("alpha", "beta", "gamma"), List.of(10, 11, 12),
                        List.of("excluded"), List.of(13))));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        FeatureDecisionRow decisionRow = row("8", feature, defJson(feature, "Message Body"));

        // Only 2 of 3 required leaves present -> required side NOT satisfied -> no result at all.
        ScanMessage partialMessage = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "alpha and beta but not the third", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);
        assertThat(orchestrator.scannerFor(partialMessage).scan(decisionRow)).isEmpty();

        // All 3 required leaves present, excluded absent -> matches, correct term_id.
        ScanMessage fullMessage = new ScanMessage("msg-102",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "alpha beta gamma all present here", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);
        List<TermMatchResult> results = orchestrator.scannerFor(fullMessage).scan(decisionRow);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).termId()).isEqualTo(feature + "::9");
        assertThat(results.get(0).matches()).hasSize(3); // one highlight per required leaf
    }

    @Test
    @DisplayName("Decomposed term (no AND NOT) term_regex_pattern is the REAL, readable pattern text " +
                 "from metadata, NOT the unreadable native COMBINATION formula string (e.g. '(10&11&12)') " +
                 "that the matched Expression's own text would otherwise show")
    void decomposedTerm_termRegexPatternIsReadable_notCombinationFormula() {
        String feature = "lex_decomp-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("(10&11&12)", EnumSet.of(ExpressionFlag.COMBINATION), 2),
                new Expression("alpha", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 10),
                new Expression("beta", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 11),
                new Expression("gamma", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 12));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_decomp-1.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_decomp-1-compile-results.json",
                wrapResults(simpleTermJson(feature, 2, "alpha", "beta", "gamma")));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "alpha beta gamma present", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow decisionRow = row("9", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).termRegexPattern())
                .as("must NOT be the raw, unreadable combination formula")
                .doesNotContain("(10&11&12)");
        assertThat(results.get(0).termRegexPattern()).contains("alpha");
    }

    @Test
    @DisplayName("Multiple terms (simple, decomposed, AND NOT) in the SAME feature are each correctly " +
                 "and independently resolved and evaluated in one scan")
    void mixedFeature_allTermTypesCorrectlyResolved() {
        String feature = "lex_mixed-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("simple term", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1),
                new Expression("required", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 20),
                new Expression("excluded", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 21));

        HyperscanDatabaseLoader dbLoader = new HyperscanDatabaseLoader(
                Map.of(feature, "gs://bucket/lex_mixed-1.hdb"), streamerFor(dbBytes), 10);
        TermMetadataLoader metaLoader = metadataLoader(feature, "gs://bucket/lex_mixed-1-compile-results.json",
                wrapResults(
                        simpleTermJson(feature, 1, "simple term"),
                        andNotTermJson(feature, 4, List.of("required"), List.of(20), List.of("excluded"), List.of(21))));
        FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(dbLoader, metaLoader, null);

        // "simple term" and "required" both present, "excluded" absent -> BOTH terms match.
        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, "here is a simple term and also required", null, null),
                List.of(), new MessageProcessing("2026-08-16", "10"), "ds1", true);

        FeatureDecisionRow decisionRow = row("10", feature, defJson(feature, "Message Body"));
        List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

        assertThat(results).hasSize(2);
        Set<String> termIds = new HashSet<>();
        for (TermMatchResult r : results) termIds.add(r.termId());
        assertThat(termIds).containsExactlyInAnyOrder(feature + "::1", feature + "::4");
    }
}
