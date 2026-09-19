package com.db.macs3.ecomms.spectre.decision;

import com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.hyperscan.TermIdBuilder;
import com.db.macs3.ecomms.spectre.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.model.message.*;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FeatureScanOrchestrator}.
 *
 * <h2>Confirmed gap this test class covers: AND NOT term_id / hit evaluation</h2>
 * <p>An earlier version of this class (and {@code FeatureScanOrchestrator})
 * assumed every PASS term's reportable Hyperscan expression id was always
 * its own term number, whether or not it needed AND NOT. Confirmed BROKEN
 * once the Compile Service's AND NOT fix removed native COMBINATION for AND
 * NOT terms — see TermExpressionMetadata class Javadoc (Lexicon
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
 * declares, producing genuine {@code .hdb}-format bytes, zipped up alongside
 * each test's term-metadata JSON via {@link #bundleLoader} — exercising the
 * same real zip-bundle download+extract+compile/scan path a live Dataproc
 * job would (see {@code HyperscanBundleLoader} class Javadoc for why the
 * Compile Service now writes one zip per feature instead of two separate
 * files), including real {@code COMBINATION}/{@code QUIET} semantics for
 * the decomposed-term tests below.
 *
 * <h2>{@code FeatureScanOrchestrator} is now {@code AutoCloseable}</h2>
 * <p>It owns a single, real, native {@code Scanner} for its whole lifetime
 * (see that class's own Javadoc "Performance" section — reused across every
 * scan call rather than constructed per call) — every test below opens one
 * via try-with-resources so its native scratch/callback are released
 * promptly rather than relying on the JVM exiting at the end of the suite.
 */
@DisplayName("FeatureScanOrchestrator")
class FeatureScanOrchestratorTest {

    /**
     * Compiles {@code expressions} into a real Hyperscan {@link Database} and
     * serializes it via {@link Database#save}, in the exact format
     * {@link Database#load} (and therefore {@code HyperscanBundleLoader})
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

    /** Builds a real zip bundle: {@code <feature>.hdb} + {@code <feature>-compile-results.json} entries. */
    private static byte[] zipOf(String feature, byte[] dbBytes, String metadataJson) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry(TermIdBuilder.hdbFileName(feature)));
                zip.write(dbBytes);
                zip.closeEntry();

                zip.putNextEntry(new ZipEntry(TermIdBuilder.termMetadataFileName(feature)));
                zip.write(metadataJson.getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds a {@link HyperscanBundleLoader} whose {@code feature -> zip path} map and
     * streamer directly return a real zip bundle (built via {@link #zipOf}) containing
     * both {@code dbBytes} and {@code metadataJson} — mirrors how a live job's single
     * broadcast map now resolves both artifacts from one GCS object.
     */
    private static HyperscanBundleLoader bundleLoader(String feature, String zipPath, byte[] dbBytes, String metadataJson) {
        byte[] zipBytes = zipOf(feature, dbBytes, metadataJson);
        return new HyperscanBundleLoader(Map.of(feature, zipPath), path -> new ByteArrayInputStream(zipBytes), 10);
    }

    private static final LocalDate SOME_DATE = LocalDate.parse("2026-08-16");

    private static FeatureDecisionRow row(String featureId, String featuresToApply, String defJson) {
        return new FeatureDecisionRow("proc-1", "msg-101", SOME_DATE, "Lexicon-Tagging",
                "lexicon", Long.parseLong(featureId), featureId + "-name", null, featuresToApply,
                false, null, defJson, SOME_DATE, "101");
    }

    private static String defJson(String feature, String... scopes) {
        StringBuilder scopeArr = new StringBuilder("[");
        for (int i = 0; i < scopes.length; i++) {
            if (i > 0) scopeArr.append(",");
            scopeArr.append("\"").append(scopes[i]).append("\"");
        }
        scopeArr.append("]");
        return "{\"featureId\":\"1\",\"featureName\":\"x\",\"featureType\":\"lexicon\",\"isNoiseReduction\":\"N\","
                + "\"body\":{\"id\":1,\"lexiconName\":\"" + feature + "\",\"objectId\":\"1\",\"totalTermsCount\":5,"
                + "\"minimumHits\":1,\"scope\":" + scopeArr + "}}";
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

    /**
     * A NEAR/FOLLOWEDBY {@code resolvedPatterns} term's compile-results JSON
     * entry — decomposed {@code regexPattern} leaves, a native COMBINATION
     * pre-filter id, and the {@code patternMapping} referencing the leaves'
     * own (QUIET) ids, mirroring the real sample shape exactly.
     */
    private static String resolvedPatternsChainTermJson(String feature, int termNumber, List<String> leaves,
                                                          String resolvedPatterns, int hyperscanExpressionId,
                                                          List<Integer> leafIds) {
        StringBuilder idList = new StringBuilder();
        for (int i = 0; i < leafIds.size(); i++) {
            if (i > 0) idList.append("&");
            idList.append(leafIds.get(i));
        }
        return """
            {"termId": "%s::%d", "compilationStatus": "PASS", "regexPattern": [%s],
             "requiresExclusionCheck": false, "resolvedPatterns": "%s",
             "hyperscanExpressionId": %d, "patternMapping": "(%s)"}
            """.formatted(feature, termNumber, quotedCsv(leaves.toArray(new String[0])),
                    resolvedPatterns, hyperscanExpressionId, idList);
    }

    // ── Pre-existing behaviour, unaffected by the fix ───────────────────────────

    @Test
    @DisplayName("only scans the areas the feature's scope covers")
    void respectsScopeAreaSelection() {
        String feature = "lex_bomb-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_bomb-1.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "bomb")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "there is a bomb in the body", "bomb in subject too", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in attachment too")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow bodyOnlyRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(bodyOnlyRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.MESSAGE_BODY);
        }
    }

    @Test
    @DisplayName("merges matches for the same term across multiple areas into ONE TermMatchResult")
    void mergesMatchesAcrossAreas() {
        String feature = "lex_bomb-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 7));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_bomb-2.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 7, "bomb")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "there is a bomb in the body", "bomb in subject too", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in attachment too")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow allScopeRow = row("2", feature, defJson(feature, "subject", "Message Body", "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(allScopeRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(3);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::7");

            Set<MatchArea> areas = new HashSet<>();
            for (var matcher : results.getFirst().getMatches()) areas.add(matcher.getArea());
            assertThat(areas).hasSize(3);
        }
    }

    @Test
    @DisplayName("skips an attachment exceeding the configured size limit entirely")
    void skipsOversizedAttachment() {
        String feature = "lex_bomb-3";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_bomb-3.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "bomb")));

        ScanMessage message = new ScanMessage("msg-101",
                new MessageSource("chat", "src", "sys", "conv-1"),
                new MessageContent(null, null, null, null),
                List.of(new MessageAttachment("att-1", null, "file.txt", "bomb in a long attachment")),
                new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
        FeatureDecisionRow attachOnlyRow = row("3", feature, defJson(feature, "Attachment"));

        try (FeatureScanOrchestrator limited = new FeatureScanOrchestrator(loader, 5L)) { // 5-byte limit
            assertThat(limited.scannerFor(message).scan(attachOnlyRow)).isEmpty();
        }

        try (FeatureScanOrchestrator unlimited = new FeatureScanOrchestrator(loader, null)) {
            assertThat(unlimited.scannerFor(message).scan(attachOnlyRow)).hasSize(1);
        }
    }

    @Test
    @DisplayName("termRegexPattern is populated from term metadata (the Compile Service's own " +
                 "translatedPattern, not the raw matched Expression text)")
    void termRegexPatternPopulatedFromMetadata() {
        String feature = "lex_bomb-4";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_bomb-4.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "bomb")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "there is a bomb in the body", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow bodyOnlyRow = row("4", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(bodyOnlyRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermRegexPattern()).isEqualTo("bomb");
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::1");
        }
    }

    @Test
    @DisplayName("The expression id -> termId mapping works uniformly for high, non-index-like term " +
                 "numbers too — the Compile Service's id scheme guarantees a non-AND-NOT PASS term's " +
                 "reportable id is always its own term number, whatever that number is")
    void expressionIdMappingWorksForHighTermNumbers() {
        String feature = "lex_bomb-5";
        byte[] dbBytes = compileAndSerialize(
                new Expression("insider", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 47));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_bomb-5.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 47, "insider")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "insider trading", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("5", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::47");
        }
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

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_andnot-1.zip", dbBytes,
                wrapResults(andNotTermJson(feature, 3, List.of("insider"), List.of(5), List.of("disclosed"), List.of(6))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "insider trading occurred and was later disclosed to the board", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("6", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results)
                    .as("required present AND excluded ALSO present -> term must NOT appear in results")
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("AND NOT term correctly matches with the CORRECT term_id when excluded is absent " +
                 "(not a raw auxiliary expression id like 5 or 6)")
    void andNotTerm_correctTermIdWhenMatched() {
        String feature = "lex_andnot-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("insider", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 5),
                new Expression("disclosed", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 6));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_andnot-2.zip", dbBytes,
                wrapResults(andNotTermJson(feature, 3, List.of("insider"), List.of(5), List.of("disclosed"), List.of(6))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "insider trading occurred yesterday", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("7", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId())
                    .as("must be the term's OWN number (3), never a raw auxiliary id like 5 or 6")
                    .isEqualTo(feature + "::3");
            assertThat(results.getFirst().getMatches().getFirst().getSpan().getMatchedText()).isEqualTo("insider");
        }
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

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_andnot-3.zip", dbBytes,
                wrapResults(andNotTermJson(feature, 9, List.of("alpha", "beta", "gamma"), List.of(10, 11, 12),
                        List.of("excluded"), List.of(13))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            FeatureDecisionRow decisionRow = row("8", feature, defJson(feature, "Message Body"));

            // Only 2 of 3 required leaves present -> required side NOT satisfied -> no result at all.
            ScanMessage partialMessage = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "alpha and beta but not the third", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(partialMessage).scan(decisionRow)).isEmpty();

            // All 3 required leaves present, excluded absent -> matches, correct term_id.
            ScanMessage fullMessage = new ScanMessage("msg-102",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "alpha beta gamma all present here", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            List<TermMatchResult> results = orchestrator.scannerFor(fullMessage).scan(decisionRow);
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::9");
            assertThat(results.getFirst().getMatches()).hasSize(3); // one highlight per required leaf
        }
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

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_decomp-1.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 2, "alpha", "beta", "gamma")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "alpha beta gamma present", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("9", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermRegexPattern())
                    .as("must NOT be the raw, unreadable combination formula")
                    .doesNotContain("(10&11&12)");
            assertThat(results.getFirst().getTermRegexPattern()).contains("alpha");
        }
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

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_mixed-1.zip", dbBytes,
                wrapResults(
                        simpleTermJson(feature, 1, "simple term"),
                        andNotTermJson(feature, 4, List.of("required"), List.of(20), List.of("excluded"), List.of(21))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            // "simple term" and "required" both present, "excluded" absent -> BOTH terms match.
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "here is a simple term and also required", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("10", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(2);
            Set<String> termIds = new HashSet<>();
            for (TermMatchResult r : results) termIds.add(r.getTermId());
            assertThat(termIds).containsExactlyInAnyOrder(feature + "::1", feature + "::4");
        }
    }

    // ── resolvedPatterns terms: NEAR / FOLLOWEDBY / AND NOT, new schema ────────

    @Test
    @DisplayName("REGRESSION: NEAR chain term (decomposed, QUIET leaves, sample term 1 shape) resolves the " +
                 "correct term_id AND verifies the real word-distance — the native COMBINATION pre-filter " +
                 "alone is NOT sufficient, since it fires whenever both leaves are present ANYWHERE in the " +
                 "scan buffer, regardless of distance")
    void nearChainTerm_realDistanceVerified() {
        String feature = "lex_near-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("(7&8)", EnumSet.of(ExpressionFlag.COMBINATION), 1),
                new Expression("manipulate", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 7),
                new Expression("(?:price|spread|stock)", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 8));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_near-1.zip", dbBytes,
                wrapResults(resolvedPatternsChainTermJson(feature, 1,
                        List.of("manipulate", "(?:price|spread|stock)"),
                        "manipulate NEAR{5} (?:price|spread|stock)", 1, List.of(7, 8))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            FeatureDecisionRow decisionRow = row("11", feature, defJson(feature, "Message Body"));

            ScanMessage nearMessage = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "we manipulate the closing price today", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            List<TermMatchResult> results = orchestrator.scannerFor(nearMessage).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::1");
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.MESSAGE_BODY);

            // Both leaves present -> native COMBINATION still fires -> but more than 5 words apart ->
            // the real per-area regex distance check must reject it.
            ScanMessage farMessage = new ScanMessage("msg-102",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null,
                            "manipulate one two three four five six seven eight nine ten price", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(farMessage).scan(decisionRow)).isEmpty();
        }
    }

    @Test
    @DisplayName("REGRESSION: FOLLOWEDBY 3-leaf chain term (decomposed, QUIET leaves, sample term 6 shape) " +
                 "resolves the correct term_id")
    void followedByChainTerm_realMatch() {
        String feature = "lex_followedby-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("(9&10&11)", EnumSet.of(ExpressionFlag.COMBINATION), 6),
                new Expression("avoidnow", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 9),
                new Expression("frontrun", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 10),
                new Expression("danger", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 11));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_followedby-1.zip", dbBytes,
                wrapResults(resolvedPatternsChainTermJson(feature, 6,
                        List.of("avoidnow", "frontrun", "danger"),
                        "avoidnow FOLLOWEDBY{4} frontrun FOLLOWEDBY{4} danger", 6, List.of(9, 10, 11))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "please avoidnow any frontrun of the danger zone", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            FeatureDecisionRow decisionRow = row("12", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::6");
        }
    }

    @Test
    @DisplayName("REGRESSION: NEAR chain plainly ANDed with a third leaf (lexicon_research_3::2 production "
                 + "shape) loads without HyperscanBundleLoader throwing, and requires BOTH the NEAR-chain "
                 + "AND the third leaf present in the same area")
    void nearChainPlainlyAndedWithThirdLeaf_bothConditionsRequired() {
        String feature = "lex_near_and-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("(20&21&22)", EnumSet.of(ExpressionFlag.COMBINATION), 2),
                new Expression("(?:any hint|early look)", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 20),
                new Expression("(?:analyst|research)", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 21),
                new Expression("(?:about to publish|pre-publication)",
                        EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 22));

        String termJson = """
            {"termId": "%s::2", "compilationStatus": "PASS",
             "regexPattern": ["(?:any hint|early look)", "(?:analyst|research)", "(?:about to publish|pre-publication)"],
             "requiresExclusionCheck": false,
             "resolvedPatterns": "(?:any hint|early look) NEAR{5} (?:analyst|research) AND (?:about to publish|pre-publication)",
             "hyperscanExpressionId": 2, "patternMapping": "(20&21&22)"}
            """.formatted(feature);
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_near_and-1.zip", dbBytes,
                wrapResults(termJson));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            FeatureDecisionRow decisionRow = row("16", feature, defJson(feature, "Message Body"));

            // NEAR-chain satisfied (within 5 words) AND the third leaf also present -> matches.
            ScanMessage bothPresent = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "any hint from the analyst about to publish soon", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            List<TermMatchResult> results = orchestrator.scannerFor(bothPresent).scan(decisionRow);
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::2");

            // NEAR-chain satisfied, but the third leaf is ABSENT -> plain AND (not OR) means no match.
            ScanMessage onlyNearChain = new ScanMessage("msg-102",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "any hint from the analyst today", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(onlyNearChain).scan(decisionRow)).isEmpty();

            // Third leaf present, but the NEAR-chain is not (leaves too far apart) -> no match.
            ScanMessage onlyThirdLeaf = new ScanMessage("msg-103",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "pre-publication of something unrelated", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(onlyThirdLeaf).scan(decisionRow)).isEmpty();
        }
    }

    @Test
    @DisplayName("AND NOT resolvedPatterns term: required alone matches, excluded alone does not, and " +
                 "both together are correctly suppressed by the per-area regex AND NOT evaluation")
    void andNotResolvedPatterns_requiredExcludedInteraction() {
        String feature = "lex_andnot-resolved-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("reqleaf", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 30),
                new Expression("exclleaf", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 31));

        String termJson = """
            {"termId": "%s::7", "compilationStatus": "PASS", "regexPattern": ["reqleaf", "exclleaf"],
             "requiresExclusionCheck": true, "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
             "requiredExpressionIds": [30], "excludedExpressionIds": [31]}
            """.formatted(feature);
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_andnot-resolved-1.zip", dbBytes,
                wrapResults(termJson));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            FeatureDecisionRow decisionRow = row("13", feature, defJson(feature, "Message Body"));

            ScanMessage onlyExcluded = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "this message has exclleaf only", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(onlyExcluded).scan(decisionRow)).isEmpty();

            ScanMessage onlyRequired = new ScanMessage("msg-102",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "this message has reqleaf only", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            List<TermMatchResult> results = orchestrator.scannerFor(onlyRequired).scan(decisionRow);
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::7");

            ScanMessage both = new ScanMessage("msg-103",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "this message has reqleaf and exclleaf both", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            assertThat(orchestrator.scannerFor(both).scan(decisionRow))
                    .as("required present AND excluded ALSO present -> term must NOT appear in results")
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("Mixed feature: a legacy simple term and a new resolvedPatterns NEAR term coexist " +
                 "correctly and are independently resolved in one scan")
    void mixedFeature_legacyAndResolvedPatternsTermsCoexist() {
        String feature = "lex_mixed-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("legacyword", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1),
                new Expression("(7&8)", EnumSet.of(ExpressionFlag.COMBINATION), 2),
                new Expression("manipulate", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 7),
                new Expression("(?:price|spread|stock)", EnumSet.of(ExpressionFlag.QUIET, ExpressionFlag.CASELESS), 8));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_mixed-2.zip", dbBytes,
                wrapResults(
                        simpleTermJson(feature, 1, "legacyword"),
                        resolvedPatternsChainTermJson(feature, 2,
                                List.of("manipulate", "(?:price|spread|stock)"),
                                "manipulate NEAR{5} (?:price|spread|stock)", 2, List.of(7, 8))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "legacyword here, and manipulate the price too", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);
            FeatureDecisionRow decisionRow = row("14", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(2);
            Set<String> termIds = new HashSet<>();
            for (TermMatchResult r : results) termIds.add(r.getTermId());
            assertThat(termIds).containsExactlyInAnyOrder(feature + "::1", feature + "::2");
        }
    }

    // ── CHAT/VOICE channel-specific MESSAGE_BODY handling ───────────────────────

    /**
     * The exact CHAT-type {@code content.raw_text} sample from the requirement: an HTML table
     * report with 3 rows, only the LAST TWO of which have a non-empty {@code message_text} cell
     * ({@code "Hi There"}, then {@code "<p>Can you share market price?</p>"}) — the first row's
     * cell is empty (an attachment-only event).
     */
    private static final String CHAT_TABLE_RAW_TEXT =
            "<html><head><style>.message-text-cell {white-space: pre-wrap;word-break: break-word;}"
            + "</style></head><body><table border=\"1\" cellpadding=\"5\" cellspacing=\"0\"><thead><tr>"
            + "<th>room_name</th><th>event_trigg_from</th><th>company_name</th><th>evt_id</th>"
            + "<th>msg_type</th><th>event_time</th><th>email</th><th>message_text</th>"
            + "<th>event_type</th><th>srcSysAttId</th><th>att_filename</th><th>att_file_size</th>"
            + "<th>broadcast_id</th></tr></thead><tbody><tr><td class=\"room_name\"></td>"
            + "<td class=\"event_trigg_from\"></td><td class=\"company_name\">DEUTSCHE BANK AG, LO</td>"
            + "<td class=\"evt_id\">34644ea1-b6af-3448-8a5e-74e7eab27434</td><td class=\"msg_type\"></td>"
            + "<td class=\"event_time\">2016-07-13T07:14:09Z</td>"
            + "<td class=\"email\">lars.van-leeuwenstijn@db.com</td>"
            + "<td class=\"message-text-cell message_text\"></td><td class=\"event_type\">Attachment</td>"
            + "<td class=\"srcSysAttId\">File_0.xlsx</td><td class=\"att_filename\">File_0.xlsx</td>"
            + "<td class=\"att_file_size\">30</td><td class=\"broadcast_id\"></td></tr>  <tr>"
            + "<td class=\"room_name\"></td><td class=\"event_trigg_from\"></td>"
            + "<td class=\"company_name\">DEUTSCHE BANK SECURI</td>"
            + "<td class=\"evt_id\">2f48e35e-5997-3191-99e9-8c1789b3db90</td><td class=\"msg_type\"></td>"
            + "<td class=\"event_time\">2016-09-28T13:08:14Z</td>"
            + "<td class=\"email\">aakanksha.kadam@db.com</td>"
            + "<td class=\"message-text-cell message_text\">Hi There</td>"
            + "<td class=\"event_type\">Message</td><td class=\"srcSysAttId\"></td>"
            + "<td class=\"att_filename\"></td><td class=\"att_file_size\"></td>"
            + "<td class=\"broadcast_id\"></td></tr><tr><td class=\"room_name\"></td>"
            + "<td class=\"event_trigg_from\"></td><td class=\"company_name\">DEUTSCHE BANK SECURI</td>"
            + "<td class=\"evt_id\">2f48e35e-5997-3191-99e9-8c1789b3db90</td><td class=\"msg_type\"></td>"
            + "<td class=\"event_time\">2016-09-28T13:08:14Z</td>"
            + "<td class=\"email\">aakanksha.kadam@db.com</td>"
            + "<td class=\"message-text-cell message_text\"><p>Can you share market price?</p></td>"
            + "<td class=\"event_type\">Message</td><td class=\"srcSysAttId\"></td>"
            + "<td class=\"att_filename\"></td><td class=\"att_file_size\"></td>"
            + "<td class=\"broadcast_id\"></td></tr></tbody></table></body></html>";

    @Test
    @DisplayName("CHAT channel: matches 'market price' inside a message_text <td>, and the reported "
                 + "start/end in raw_text point EXACTLY at 'market price' — never at an unrelated column")
    void chatChannel_matchesInsideMessageTextCell_positionResolvesToRawText() {
        String feature = "lex_chat-1";
        byte[] dbBytes = compileAndSerialize(
                new Expression("market price", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1),
                new Expression("DEUTSCHE BANK", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 2));

        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_chat-1.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "market price"),
                        simpleTermJson(feature, 2, "DEUTSCHE BANK")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, CHAT_TABLE_RAW_TEXT, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            // Only the message_text-cell term matches — "DEUTSCHE BANK" only ever appears in the
            // company_name column, which must never reach Hyperscan for a CHAT/VOICE message.
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getTermId()).isEqualTo(feature + "::1");

            var match = results.getFirst().getMatches().getFirst();
            assertThat(match.getArea()).isEqualTo(MatchArea.MESSAGE_BODY);
            int start = match.getSpan().getStartCharIndex();
            int end = match.getSpan().getEndCharIndex();
            assertThat(CHAT_TABLE_RAW_TEXT.substring(start, end)).isEqualTo("market price");
        }
    }

    @Test
    @DisplayName("CHAT channel is case-insensitive against channel_name — 'CHAT' behaves identically to 'chat'")
    void chatChannel_caseInsensitive() {
        String feature = "lex_chat-2";
        byte[] dbBytes = compileAndSerialize(
                new Expression("market price", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_chat-2.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "market price")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("CHAT", "src", "sys", "conv-1"),
                    new MessageContent(null, CHAT_TABLE_RAW_TEXT, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
        }
    }

    @Test
    @DisplayName("VOICE channel: matches inside a message_text <td> the same way CHAT does")
    void voiceChannel_matchesInsideMessageTextCell() {
        String feature = "lex_voice-1";
        String rawText = "<html><body><table><tbody><tr>"
                + "<td class=\"room_name\"></td>"
                + "<td class=\"message-text-cell message_text\">This is very strong language</td>"
                + "<td class=\"event_type\">speechtotext</td></tr><tr>"
                + "<td class=\"room_name\"></td>"
                + "<td class=\"message-text-cell message_text\">Cats and dogs hate each other</td>"
                + "<td class=\"event_type\">speechtotext</td></tr>"
                + "</tbody></table></body></html>";

        byte[] dbBytes = compileAndSerialize(
                new Expression("strong language", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_voice-1.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "strong language")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            var match = results.getFirst().getMatches().getFirst();
            int start = match.getSpan().getStartCharIndex();
            int end = match.getSpan().getEndCharIndex();
            assertThat(rawText.substring(start, end)).isEqualTo("strong language");
        }
    }

    @Test
    @DisplayName("CHAT channel falls back to whole-text stripping when raw_text has no message_text "
                 + "<td> at all — e.g. plain, non-table text — so existing plain-text CHAT messages "
                 + "keep matching exactly as before this feature existed")
    void chatChannel_noMessageTextCell_fallsBackToPlainStripping() {
        String feature = "lex_chat-3";
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_chat-3.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "bomb")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, "there is a bomb in the body", null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
        }
    }

    // ── CHAT/VOICE with varying row counts — matched text position/length correctness ──────────

    /**
     * Builds a minimal CHAT/VOICE-shaped {@code raw_text} table with one row per entry in
     * {@code cellTexts} (in order), each row's {@code message_text} cell holding that entry —
     * lets a test vary "how many rows/cells precede the matching one" just by varying this
     * array's length, without hand-building a full table each time.
     */
    private static String messageTextTableRawText(String... cellTexts) {
        StringBuilder sb = new StringBuilder("<html><body><table><tbody>");
        for (String cellText : cellTexts) {
            sb.append("<tr><td class=\"room_name\"></td>")
              .append("<td class=\"message-text-cell message_text\">").append(cellText).append("</td>")
              .append("<td class=\"event_type\">Message</td></tr>");
        }
        sb.append("</tbody></table></body></html>");
        return sb.toString();
    }

    /**
     * Asserts a single match's {@code startCharIndex}/{@code endCharIndex}/length resolve back
     * to EXACTLY {@code expectedText} in {@code rawText} — the position/length correctness check
     * this whole test group exists for.
     */
    private static void assertSingleMatchPositionAndLength(List<TermMatchResult> results, String rawText,
                                                            String expectedText) {
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().getMatches()).hasSize(1);
        var span = results.getFirst().getMatches().getFirst().getSpan();
        assertThat(span.length()).as("matched span length").isEqualTo(expectedText.length());
        assertThat(rawText.substring(span.getStartCharIndex(), span.getEndCharIndex()))
                .as("raw_text.substring(startCharIndex, endCharIndex)").isEqualTo(expectedText);
        assertThat(span.getMatchedText()).isEqualTo(expectedText);
    }

    @Test
    @DisplayName("VOICE, single row: position/length resolve exactly to the one message_text cell's content")
    void voiceChannel_singleRow_positionAndLengthCorrect() {
        String feature = "lex_voice-2";
        String rawText = messageTextTableRawText("This call contains insider trading information");

        byte[] dbBytes = compileAndSerialize(
                new Expression("insider trading", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_voice-2.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "insider trading")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "insider trading");
        }
    }

    @Test
    @DisplayName("VOICE, 3 rows: match is in the LAST row — position must skip past the two preceding "
                 + "rows' own message_text cells, not just their combined length coincidentally")
    void voiceChannel_threeRows_matchInLastRow_positionAndLengthCorrect() {
        String feature = "lex_voice-3";
        String rawText = messageTextTableRawText(
                "Cats and dogs hate each other",
                "This is very strong language",
                "Please transfer the funds to account 12345");

        byte[] dbBytes = compileAndSerialize(
                new Expression("transfer the funds", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_voice-3.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "transfer the funds")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "transfer the funds");
        }
    }

    @Test
    @DisplayName("VOICE, 5 rows: match is in a MIDDLE row (3rd of 5) — position must be correct with "
                 + "both preceding AND following rows present")
    void voiceChannel_fiveRows_matchInMiddleRow_positionAndLengthCorrect() {
        String feature = "lex_voice-4";
        String rawText = messageTextTableRawText(
                "alpha bravo",
                "charlie delta",
                "insider trading is illegal and unethical",
                "echo foxtrot",
                "golf hotel");

        byte[] dbBytes = compileAndSerialize(
                new Expression("insider trading", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_voice-4.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "insider trading")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "insider trading");
        }
    }

    @Test
    @DisplayName("VOICE, 4 rows: the SAME term matches in TWO different rows — each occurrence's "
                 + "position/length is resolved independently and correctly, not just the first")
    void voiceChannel_fourRows_matchesInTwoRows_eachPositionAndLengthCorrect() {
        String feature = "lex_voice-5";
        String rawText = messageTextTableRawText(
                "please wire the funds today",
                "no relevant content here",
                "we will wire the funds tomorrow instead",
                "goodbye");

        byte[] dbBytes = compileAndSerialize(
                new Expression("wire the funds", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_voice-5.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "wire the funds")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(2);
            for (var match : results.getFirst().getMatches()) {
                var span = match.getSpan();
                assertThat(span.length()).isEqualTo("wire the funds".length());
                assertThat(rawText.substring(span.getStartCharIndex(), span.getEndCharIndex()))
                        .isEqualTo("wire the funds");
            }
            // And the two occurrences are genuinely at different positions, not the same one twice.
            int firstStart = results.getFirst().getMatches().get(0).getSpan().getStartCharIndex();
            int secondStart = results.getFirst().getMatches().get(1).getSpan().getStartCharIndex();
            assertThat(firstStart).isNotEqualTo(secondStart);
        }
    }

    @Test
    @DisplayName("CHAT, 2 rows: match is in the SECOND row")
    void chatChannel_twoRows_matchInSecondRow_positionAndLengthCorrect() {
        String feature = "lex_chat-4";
        String rawText = messageTextTableRawText(
                "Hi there, how are you?",
                "Can you please share confidential information");

        byte[] dbBytes = compileAndSerialize(
                new Expression("confidential information",
                        EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_chat-4.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "confidential information")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "confidential information");
        }
    }

    @Test
    @DisplayName("CHAT, 4 rows: match is in the THIRD row, and other rows' content is never mistaken for it")
    void chatChannel_fourRows_matchInThirdRow_positionAndLengthCorrect() {
        String feature = "lex_chat-5";
        String rawText = messageTextTableRawText(
                "good morning everyone",
                "let's sync at 3pm",
                "please transfer the funds before the deadline",
                "thanks, talk soon");

        byte[] dbBytes = compileAndSerialize(
                new Expression("transfer the funds", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_chat-5.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "transfer the funds")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "transfer the funds");
        }
    }

    // ── EMAIL — plain text and HTML content, position/length correctness ───────────────────────

    @Test
    @DisplayName("EMAIL, plain text (no HTML at all): position/length are an exact, direct substring — "
                 + "no offset translation needed since there is no markup to skip over")
    void emailChannel_plainText_positionAndLengthCorrect() {
        String feature = "lex_email-1";
        String rawText = "Please transfer the funds to account 12345 immediately";

        byte[] dbBytes = compileAndSerialize(
                new Expression("transfer the funds", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_email-1.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "transfer the funds")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "transfer the funds");

            // For plain, HTML-free text the match position is a direct, un-translated index —
            // exactly rawText.indexOf(the literal), with no HTML to have skipped over.
            var span = results.getFirst().getMatches().getFirst().getSpan();
            assertThat(span.getStartCharIndex()).isEqualTo(rawText.indexOf("transfer the funds"));
        }
    }

    @Test
    @DisplayName("EMAIL, HTML content where the match itself sits entirely inside one contiguous run of "
                 + "text (no tag interrupts the matched phrase itself, only text around it): position/"
                 + "length resolve to an exact substring, same guarantee as the CHAT/VOICE cell tests above")
    void emailChannel_htmlContent_matchNotInterruptedByTags_positionAndLengthCorrect() {
        String feature = "lex_email-2";
        String rawText = "<div><p>Please see the attached statement.</p>"
                + "<p>Kindly transfer the funds to the escrow account.</p></div>";

        byte[] dbBytes = compileAndSerialize(
                new Expression("transfer the funds", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_email-2.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, "transfer the funds")));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));
            assertSingleMatchPositionAndLength(results, rawText, "transfer the funds");
        }
    }

    @Test
    @DisplayName("EMAIL, HTML content where HTML tags fall BETWEEN the matched words — the exact "
                 + "requirement worked example (Enjoy/Happy): startCharIndex/endCharIndex/matchedText "
                 + "match the values already verified independently in HtmlStrippingServiceTest")
    void emailChannel_htmlContent_matchSpansAcrossTags_positionMatchesWorkedExample() {
        String feature = "lex_email-3";
        String rawText = "<p>Enjoy</p>\n<p>Happy Birthday</p>";
        String patternText = "Enjoy(?:\\s+\\S+){0,2}\\s+Happy"; // actual regex text: Enjoy(?:\s+\S+){0,2}\s+Happy

        byte[] dbBytes = compileAndSerialize(
                new Expression(patternText, EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        // The metadata JSON's translatedPattern is JSON TEXT, not a Java string literal — every
        // backslash in patternText must be doubled so the JSON parser reconstructs the same
        // single-backslash regex text simpleTermJson's raw quotedCsv would otherwise mangle.
        HyperscanBundleLoader loader = bundleLoader(feature, "gs://bucket/lex_email-3.zip", dbBytes,
                wrapResults(simpleTermJson(feature, 1, patternText.replace("\\", "\\\\"))));

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            List<TermMatchResult> results = orchestrator.scannerFor(message)
                    .scan(row("1", feature, defJson(feature, "Message Body")));

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            var span = results.getFirst().getMatches().getFirst().getSpan();
            // Same values HtmlStrippingServiceTest.WorkedExample derives and asserts independently
            // via java.util.regex against HtmlStrippingService.strip directly — reproduced here
            // end-to-end through the real orchestrator + real Hyperscan.
            assertThat(span.getMatchedText()).isEqualTo("Enjoy Happy");
            assertThat(span.getStartCharIndex()).isEqualTo(3);
            assertThat(span.getEndCharIndex()).isEqualTo(21);
        }
    }

    // ── CHAT/VOICE + attachment: scope enforcement ──────────────────────────────
    //
    // feature_definition.body.scope gates which AREAS a feature's Hyperscan database is even
    // run against (see scopeFor/scanRow above) — this is entirely orthogonal to the CHAT/VOICE
    // message_text-cell extraction change: extraction only decides WHAT TEXT the MESSAGE_BODY
    // area contains, never whether that area (or SUBJECT/ATTACHMENT) is scanned at all. The
    // tests below lock in that the two remain independent for CHAT/VOICE messages that also
    // carry an attachment — a combination the earlier CHAT/VOICE test group never exercised.

    private static HyperscanBundleLoader bombLoader(String feature, String zipPath) {
        byte[] dbBytes = compileAndSerialize(
                new Expression("bomb", EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS), 1));
        return bundleLoader(feature, zipPath, dbBytes, wrapResults(simpleTermJson(feature, 1, "bomb")));
    }

    @Test
    @DisplayName("CHAT, scope=[subject, Attachment] (no Message Body): 'bomb' present ONLY in the "
                 + "message_text cell -> NOT a match, even though the word is genuinely in raw_text")
    void chatChannel_scopeExcludesMessageBody_termOnlyInBody_noMatch() {
        String feature = "lex_scope-1";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-1.zip");
        String rawText = messageTextTableRawText("there is a bomb in the chat body");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, "no relevant subject text here", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "nothing relevant here")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "subject", "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).isEmpty();
        }
    }

    @Test
    @DisplayName("CHAT, scope=[Attachment] only: 'bomb' present ONLY in the attachment -> IS a match")
    void chatChannel_scopeIsAttachmentOnly_termOnlyInAttachment_matches() {
        String feature = "lex_scope-2";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-2.zip");
        String rawText = messageTextTableRawText("nothing relevant in the chat body");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, "no relevant subject either", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "there is a bomb in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.ATTACHMENT);
        }
    }

    @Test
    @DisplayName("CHAT, scope=[Message Body] only: 'bomb' present ONLY in the attachment -> NOT a match, "
                 + "even though the message genuinely has an attachment containing it")
    void chatChannel_scopeIsMessageBodyOnly_termOnlyInAttachment_noMatch() {
        String feature = "lex_scope-3";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-3.zip");
        String rawText = messageTextTableRawText("nothing relevant in the chat body");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "there is a bomb in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).isEmpty();
        }
    }

    @Test
    @DisplayName("VOICE, scope=[subject] only: 'bomb' present ONLY in the message_text cell -> NOT a match")
    void voiceChannel_scopeIsSubjectOnly_termOnlyInBody_noMatch() {
        String feature = "lex_scope-4";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-4.zip");
        String rawText = messageTextTableRawText("there is a bomb in this voice transcript");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, "no relevant subject", null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "subject"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).isEmpty();
        }
    }

    @Test
    @DisplayName("VOICE, scope=[subject] only: 'bomb' present in subject AND in the message_text cell "
                 + "AND in the attachment -> matches EXACTLY ONCE, from SUBJECT only")
    void voiceChannel_scopeIsSubjectOnly_termInAllAreas_matchesSubjectOnly() {
        String feature = "lex_scope-5";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-5.zip");
        String rawText = messageTextTableRawText("there is a bomb in this voice transcript too");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, "bomb mentioned in the subject", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb mentioned in the attachment too")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "subject"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.SUBJECT);
        }
    }

    @Test
    @DisplayName("CHAT, scope=[subject, Message Body, Attachment] (all three): 'bomb' present in every "
                 + "area -> matches ALL THREE, merged into one TermMatchResult")
    void chatChannel_allScopesIncluded_termInAllThreeAreas_matchesAllThree() {
        String feature = "lex_scope-6";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-6.zip");
        String rawText = messageTextTableRawText("there is a bomb in the chat body");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, "bomb mentioned in the subject", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb mentioned in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "subject", "Message Body", "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(3);
            Set<MatchArea> areas = new HashSet<>();
            for (var match : results.getFirst().getMatches()) areas.add(match.getArea());
            assertThat(areas).containsExactlyInAnyOrder(MatchArea.SUBJECT, MatchArea.MESSAGE_BODY, MatchArea.ATTACHMENT);
        }
    }

    @Test
    @DisplayName("CHAT, scope=[Message Body, Attachment]: 'bomb' appears in a NON-message_text column "
                 + "(company_name) of the table AND in the attachment -> matches ONLY from the "
                 + "attachment; the other table column never leaks into the Message Body scan")
    void chatChannel_termInOtherTableColumn_onlyAttachmentMatches() {
        String feature = "lex_scope-7";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-7.zip");
        String rawText = "<html><body><table><tbody><tr>"
                + "<td class=\"company_name\">BOMB SQUAD HOLDINGS</td>"
                + "<td class=\"message-text-cell message_text\">nothing relevant here</td>"
                + "</tr></tbody></table></body></html>";

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "there is a bomb in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body", "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.ATTACHMENT);
        }
    }

    @Test
    @DisplayName("CHAT, scope=[Attachment] only, message has NO attachment at all: no match, and no "
                 + "exception — robustness when the in-scope area simply doesn't exist on this message")
    void chatChannel_scopeIsAttachmentOnly_noAttachmentPresent_noMatch() {
        String feature = "lex_scope-8";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-8.zip");
        String rawText = messageTextTableRawText("there is a bomb in the chat body");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("chat", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).isEmpty();
        }
    }

    @Test
    @DisplayName("VOICE, scope=[Message Body] only: 'bomb' in BOTH the message_text cell AND the "
                 + "attachment -> matches ONLY from MESSAGE_BODY, attachment excluded despite containing it too")
    void voiceChannel_scopeIsMessageBodyOnly_termInBodyAndAttachment_matchesBodyOnly() {
        String feature = "lex_scope-9";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-9.zip");
        String rawText = messageTextTableRawText("there is a bomb in this voice transcript");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("voice", "src", "sys", "conv-1"),
                    new MessageContent(null, rawText, null, null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "there is a bomb in the attachment too")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "Message Body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.MESSAGE_BODY);
        }
    }

    // ── feature_definition.body.scope: case-insensitive against the scanned area ───────────

    @Test
    @DisplayName("scope=[SUBJECT] (all uppercase): still selects the SUBJECT area")
    void scope_allUppercase_stillSelectsSubjectArea() {
        String feature = "lex_scope-10";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-10.zip");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, "nothing relevant in the body", "there is a bomb in the subject", null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "SUBJECT"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.SUBJECT);
        }
    }

    @Test
    @DisplayName("scope=[message body] (all lowercase, two-word value): still selects the MESSAGE_BODY area")
    void scope_lowercaseTwoWordValue_stillSelectsMessageBodyArea() {
        String feature = "lex_scope-11";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-11.zip");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, "there is a bomb in the body", "nothing relevant in the subject", null),
                    List.of(), new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "message body"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.MESSAGE_BODY);
        }
    }

    @Test
    @DisplayName("scope=[attachment] (all lowercase): still selects the ATTACHMENT area")
    void scope_lowercaseAttachment_stillSelectsAttachmentArea() {
        String feature = "lex_scope-12";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-12.zip");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, "nothing relevant in the body", "nothing relevant in the subject", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "there is a bomb in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "attachment"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(1);
            assertThat(results.getFirst().getMatches().getFirst().getArea()).isEqualTo(MatchArea.ATTACHMENT);
        }
    }

    @Test
    @DisplayName("scope=[SuBjEcT, MeSsAgE BoDy, aTTachMENT] (mixed casing on every value): matches all three areas")
    void scope_mixedCasingOnEveryValue_matchesAllThreeAreas() {
        String feature = "lex_scope-13";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-13.zip");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, "bomb mentioned in the body", "bomb mentioned in the subject", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb mentioned in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature,
                    defJson(feature, "SuBjEcT", "MeSsAgE BoDy", "aTTachMENT"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).hasSize(1);
            assertThat(results.getFirst().getMatches()).hasSize(3);
            Set<MatchArea> areas = new HashSet<>();
            for (var match : results.getFirst().getMatches()) areas.add(match.getArea());
            assertThat(areas).containsExactlyInAnyOrder(MatchArea.SUBJECT, MatchArea.MESSAGE_BODY, MatchArea.ATTACHMENT);
        }
    }

    @Test
    @DisplayName("scope=[SUBJECT] (uppercase) does NOT select MESSAGE_BODY or ATTACHMENT — case-insensitivity "
                 + "only widens matching for the scope's own listed values, it doesn't select every area")
    void scope_uppercaseSubjectOnly_doesNotAlsoSelectOtherAreas() {
        String feature = "lex_scope-14";
        HyperscanBundleLoader loader = bombLoader(feature, "gs://bucket/lex_scope-14.zip");

        try (FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(loader, null)) {
            ScanMessage message = new ScanMessage("msg-101",
                    new MessageSource("email", "src", "sys", "conv-1"),
                    new MessageContent(null, "bomb mentioned in the body", "nothing relevant in the subject", null),
                    List.of(new MessageAttachment("att-1", null, "file.txt", "bomb mentioned in the attachment")),
                    new MessageProcessing(LocalDate.of(2026, 8, 16), "10"), "ds1", true);

            FeatureDecisionRow decisionRow = row("1", feature, defJson(feature, "SUBJECT"));
            List<TermMatchResult> results = orchestrator.scannerFor(message).scan(decisionRow);

            assertThat(results).isEmpty();
        }
    }
}
