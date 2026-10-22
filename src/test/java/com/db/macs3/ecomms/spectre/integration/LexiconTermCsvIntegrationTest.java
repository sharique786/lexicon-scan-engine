package com.db.macs3.ecomms.spectre.integration;

import com.db.macs3.ecomms.spectre.decision.FeatureScanOrchestrator;
import com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.hyperscan.TermIdBuilder;
import com.db.macs3.ecomms.spectre.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.model.message.MessageContent;
import com.db.macs3.ecomms.spectre.model.message.MessageProcessing;
import com.db.macs3.ecomms.spectre.model.message.MessageSource;
import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * CSV-driven integration test: runs the REAL scan path ({@link FeatureScanOrchestrator} → real native
 * Hyperscan → {@code TermExpressionMetadata} → {@code ResolvedPatternAreaEvaluator}) against the compiled
 * lexicons the Compile Service produced, and checks each message's outcome against what the CSV expects.
 *
 * <h2>Inputs (all on the test classpath, under {@code src/test/resources/lexicon-integration/})</h2>
 * <ul>
 *   <li>{@code lexicon-test-cases.csv} — header + one case per row: {@code lexicon_term_id} (e.g.
 *       {@code lexicon_research_1::9}), {@code message_text}, {@code expected_outcome} ({@code HIT} /
 *       {@code NO_HIT}). Header names are matched case-insensitively with spaces/underscores interchangeable
 *       ({@code "lexicon term id"} works). RFC 4180 quoting is supported, so a message may contain commas,
 *       quotes and newlines. Extra columns are ignored.</li>
 *   <li>{@code lexicons/<feature>.hdb} + {@code lexicons/<feature>.json} — the same two entries a live
 *       {@code <feature>.zip} bundle holds. {@code <feature>} is the part of the term id before {@code ::}.</li>
 * </ul>
 *
 * <h2>Semantics</h2>
 * A case is {@code HIT} iff the scan of the message (scope: message body) returns a {@link TermMatchResult}
 * whose {@code termId} equals the case's {@code lexicon_term_id}. Other terms of the same feature that also
 * fire do not change the verdict, but are listed in the report ({@code is_target_term = false}).
 *
 * <h2>Output</h2>
 * {@code target/lexicon-integration-report/lexicon-term-integration-report.csv} — flattened, one line per
 * (case × matched term × matched span); a case with no match gets one line with empty match columns, except that
 * {@code message_text}, {@code term_description} and {@code term_regex_pattern} are still filled (the latter two
 * looked up from the target term's lexicon JSON). UTF-8
 * with BOM so Excel renders non-Latin text.
 */
@DisplayName("Lexicon term CSV integration test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LexiconTermCsvIntegrationTest {

    private static final String RESOURCE_ROOT = "/lexicon-integration/";
    private static final String CASES_RESOURCE = RESOURCE_ROOT + "lexicon-test-cases.csv";
    private static final String LEXICON_DIR = RESOURCE_ROOT + "lexicons/";
    private static final Path REPORT_PATH =
            Paths.get("target", "lexicon-integration-report", "lexicon-term-integration-report.csv");

    private static final LocalDate RUN_DATE = LocalDate.parse("2026-08-16");
    private static final String TERM_SEPARATOR = "::";

    private static final List<String> REPORT_HEADER = List.of(
            "case_no", "lexicon_term_id", "feature", "expected_outcome", "actual_outcome", "result",
            "message_text", "matched_term_id", "is_target_term", "term_description", "term_regex_pattern",
            "match_area", "attachment_id", "match_start", "match_end", "matched_text", "error");

    private final Map<String, TermExpressionMetadata> metadataByFeature = new HashMap<>();
    private final List<String[]> reportRows = Collections.synchronizedList(new ArrayList<>());

    private List<TestCase> cases;
    private HyperscanBundleLoader loader;
    private FeatureScanOrchestrator orchestrator;

    @BeforeAll
    void openOrchestrator() {
        // The "GCS" the loader streams from is the test classpath: path == feature name, bytes == a zip built
        // from <feature>.hdb + <feature>.json, so the real bundle-loading code path is exercised unchanged.
        cases = readCases();
        assertThat(cases).as("no test cases found in " + CASES_RESOURCE).isNotEmpty();
        Map<String, String> featureToPath = new HashMap<>();
        for (TestCase testCase : cases) {
            String feature = featureOf(testCase.termId());
            if (feature != null) {
                featureToPath.put(feature, feature);
            }
        }
        loader = new HyperscanBundleLoader(featureToPath,
                feature -> new ByteArrayInputStream(zipFromClasspath(feature)), 50);
        orchestrator = new FeatureScanOrchestrator(loader, null);
    }

    @AfterAll
    void closeAndWriteReport() throws IOException {
        orchestrator.close();
        loader.close();
        writeReport();
    }

    @TestFactory
    @DisplayName("each CSV row: scan outcome matches expected outcome")
    List<DynamicTest> csvCases() {
        List<DynamicTest> tests = new ArrayList<>();
        for (TestCase testCase : cases) {
            String name = "#%d %s expects %s — %s".formatted(testCase.caseNo(), testCase.termId(),
                    testCase.expected(), abbreviate(testCase.messageText()));
            tests.add(DynamicTest.dynamicTest(name, () -> runCase(testCase)));
        }
        return tests;
    }

    private void runCase(TestCase testCase) {
        String feature = featureOf(testCase.termId());
        String error = null;
        Outcome expected = Outcome.parse(testCase.expected());
        List<TermMatchResult> results = List.of();

        try {
            if (feature == null) {
                throw new IllegalArgumentException("lexicon term id '" + testCase.termId()
                        + "' is not of the form <feature>" + TERM_SEPARATOR + "<n>");
            }
            if (expected == null) {
                throw new IllegalArgumentException("expected outcome '" + testCase.expected()
                        + "' is neither HIT nor NO_HIT");
            }
            results = scan(feature, testCase.messageText());
        } catch (RuntimeException e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        if (error != null) {
            addReportRows(testCase, feature, expected, null, "ERROR", List.of(), error);
            fail("Case #" + testCase.caseNo() + " (" + testCase.termId() + ") could not be evaluated — " + error);
        }

        boolean targetHit = results.stream().anyMatch(r -> testCase.termId().equals(r.getTermId()));
        Outcome actual = targetHit ? Outcome.HIT : Outcome.NO_HIT;
        boolean pass = actual == expected;
        addReportRows(testCase, feature, expected, actual, pass ? "PASS" : "FAIL", results, null);

        assertThat(actual)
                .as("Case #%d: term %s, message \"%s\", other terms hit: %s", testCase.caseNo(), testCase.termId(),
                        abbreviate(testCase.messageText()), otherTermIds(testCase.termId(), results))
                .isEqualTo(expected);
    }

    private List<TermMatchResult> scan(String feature, String messageText) {
        ScanMessage message = new ScanMessage("it-msg", new MessageSource("email", "src", "sys", "conv-1"),
                new MessageContent(null, messageText, null), List.of(),
                new MessageProcessing(RUN_DATE, "10"), "ds1", true);
        return orchestrator.scannerFor(message).scan(decisionRow(feature));
    }

    private static FeatureDecisionRow decisionRow(String feature) {
        String defJson = "{\"featureId\":\"1\",\"featureName\":\"x\",\"featureType\":\"lexicon\",\"isNoiseReduction\":\"N\","
                + "\"body\":{\"id\":1,\"lexiconName\":\"" + feature + "\",\"objectId\":\"1\",\"totalTermsCount\":5,"
                + "\"minimumHits\":1,\"scope\":[\"Message Body\"]}}";
        return new FeatureDecisionRow("proc-1", "it-msg", RUN_DATE, "Lexicon-Tagging", "lexicon", 1L, "1-name",
                null, feature, false, null, defJson, RUN_DATE, "101");
    }

    // ------------------------------------------------------------------------------------------- inputs

    private record TestCase(int caseNo, String termId, String messageText, String expected) {
    }

    private enum Outcome {
        HIT, NO_HIT;

        static Outcome parse(String raw) {
            if (raw == null) {
                return null;
            }
            return switch (raw.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z]", "")) {
                case "HIT" -> HIT;
                case "NOHIT" -> NO_HIT;
                default -> null;
            };
        }
    }

    private static String featureOf(String termId) {
        if (termId == null) {
            return null;
        }
        int idx = termId.lastIndexOf(TERM_SEPARATOR);
        return idx > 0 ? termId.substring(0, idx) : null;
    }

    private static List<TestCase> readCases() {
        String csv = new String(readClasspath(CASES_RESOURCE), StandardCharsets.UTF_8);
        List<List<String>> rows = parseCsv(csv);
        assertThat(rows).as("CSV must have a header row").isNotEmpty();

        List<String> header = rows.getFirst().stream().map(LexiconTermCsvIntegrationTest::normaliseHeader).toList();
        int termCol = requiredColumn(header, "lexicon_term_id");
        int textCol = requiredColumn(header, "message_text");
        int outcomeCol = requiredColumn(header, "expected_outcome");

        List<TestCase> cases = new ArrayList<>();
        for (int i = 1; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            if (row.stream().allMatch(String::isBlank)) {
                continue;
            }
            cases.add(new TestCase(cases.size() + 1, cell(row, termCol).trim(), cell(row, textCol),
                    cell(row, outcomeCol).trim()));
        }
        return cases;
    }

    private static String normaliseHeader(String header) {
        return header.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
    }

    private static int requiredColumn(List<String> header, String name) {
        int idx = header.indexOf(name);
        assertThat(idx).as("CSV header is missing column '" + name + "' (found " + header + ")").isGreaterThanOrEqualTo(0);
        return idx;
    }

    private static String cell(List<String> row, int idx) {
        return idx < row.size() ? row.get(idx) : "";
    }

    /** Minimal RFC 4180 parser: quoted fields, {@code ""} escapes, embedded commas/newlines, CRLF or LF, leading BOM. */
    static List<List<String>> parseCsv(String text) {
        if (!text.isEmpty() && text.charAt(0) == '﻿') {
            text = text.substring(1);
        }
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;
        boolean fieldStarted = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field.append(c);
                }
                continue;
            }
            switch (c) {
                case '"' -> {
                    inQuotes = true;
                    fieldStarted = true;
                }
                case ',' -> {
                    row.add(field.toString());
                    field.setLength(0);
                    fieldStarted = true;
                }
                case '\r', '\n' -> {
                    if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') {
                        i++;
                    }
                    if (fieldStarted || field.length() > 0 || !row.isEmpty()) {
                        row.add(field.toString());
                        rows.add(row);
                    }
                    row = new ArrayList<>();
                    field.setLength(0);
                    fieldStarted = false;
                }
                default -> {
                    field.append(c);
                    fieldStarted = true;
                }
            }
        }
        if (inQuotes) {
            throw new IllegalArgumentException("CSV ends inside an unterminated quoted field");
        }
        if (fieldStarted || field.length() > 0 || !row.isEmpty()) {
            row.add(field.toString());
            rows.add(row);
        }
        return rows;
    }

    private static byte[] readClasspath(String resource) {
        try (InputStream in = LexiconTermCsvIntegrationTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Test resource not found on classpath: " + resource);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] zipFromClasspath(String feature) {
        byte[] hdb = readClasspath(LEXICON_DIR + TermIdBuilder.hdbFileName(feature));
        byte[] json = readClasspath(LEXICON_DIR + TermIdBuilder.termMetadataFileName(feature));
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry(TermIdBuilder.hdbFileName(feature)));
                zip.write(hdb);
                zip.closeEntry();
                zip.putNextEntry(new ZipEntry(TermIdBuilder.termMetadataFileName(feature)));
                zip.write(json);
                zip.closeEntry();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------------------------------- report

    private void addReportRows(TestCase testCase, String feature, Outcome expected, Outcome actual, String result,
                               List<TermMatchResult> results, String error) {
        String expectedText = expected != null ? expected.name() : testCase.expected();
        String actualText = actual != null ? actual.name() : "";
        List<String[]> rows = new ArrayList<>();

        for (TermMatchResult termResult : results) {
            boolean target = testCase.termId().equals(termResult.getTermId());
            for (AreaMatch match : termResult.getMatches()) {
                MatchSpan span = match.getSpan();
                rows.add(reportRow(testCase, feature, expectedText, actualText, result,
                        termResult.getTermId(), String.valueOf(target),
                        termResult.getTermDescription(), termResult.getTermRegexPattern(),
                        match.getArea() != null ? match.getArea().name() : "", match.getAttachmentId(),
                        String.valueOf(span.getStartCharIndex()), String.valueOf(span.getEndCharIndex()),
                        span.getMatchedText(), error));
            }
        }
        if (rows.isEmpty()) {
            // No match at all: nothing came back from the scan, so look the target term up in the lexicon's own
            // metadata so the report still shows what the term is (description + regex) next to the message text.
            String[] termInfo = termInfo(feature, testCase.termId());
            rows.add(reportRow(testCase, feature, expectedText, actualText, result,
                    "", "", termInfo[0], termInfo[1], "", "", "", "", "", error));
        }
        reportRows.addAll(rows);
    }

    /**
     * @return {@code {term_description, term_regex_pattern}} for {@code termId} as the engine itself would report
     * them (same {@link TermExpressionMetadata} parse the scan uses); empty strings when the term or its metadata
     * cannot be resolved (e.g. an unparseable lexicon JSON — that case already surfaces as an ERROR row).
     */
    private String[] termInfo(String feature, String termId) {
        String[] none = {"", ""};
        if (feature == null || termId == null) {
            return none;
        }
        try {
            TermExpressionMetadata metadata = metadataByFeature.computeIfAbsent(feature, f -> TermExpressionMetadata.parse(f,
                    new String(readClasspath(LEXICON_DIR + TermIdBuilder.termMetadataFileName(f)), StandardCharsets.UTF_8)));
            int termNumber = Integer.parseInt(termId.substring(termId.lastIndexOf(TERM_SEPARATOR) + TERM_SEPARATOR.length()).trim());
            TermExpressionMetadata.TermEntry entry = metadata.getByTermNumber().get(termNumber);
            if (entry == null) {
                return none;
            }
            return new String[]{entry.getTermDescription(), entry.getTermRegexPattern()};
        } catch (RuntimeException e) {
            return none;
        }
    }

    private static String[] reportRow(TestCase testCase, String feature, String expected, String actual, String result,
                                      String matchedTermId, String isTarget, String description, String regex,
                                      String area, String attachmentId, String start, String end,
                                      String matchedText, String error) {
        return new String[]{String.valueOf(testCase.caseNo()), testCase.termId(), feature, expected, actual, result,
                testCase.messageText(), matchedTermId, isTarget, description, regex, area, attachmentId,
                start, end, matchedText, error};
    }

    private void writeReport() throws IOException {
        StringBuilder csv = new StringBuilder("﻿");
        appendCsvLine(csv, REPORT_HEADER.toArray(new String[0]));
        for (String[] row : reportRows) {
            appendCsvLine(csv, row);
        }
        Files.createDirectories(REPORT_PATH.getParent());
        Files.writeString(REPORT_PATH, csv, StandardCharsets.UTF_8);
        System.out.println("Lexicon integration report written to " + REPORT_PATH.toAbsolutePath());
    }

    private static void appendCsvLine(StringBuilder out, String[] values) {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(quote(values[i]));
        }
        out.append("\r\n");
    }

    private static String quote(String value) {
        if (value == null) {
            return "";
        }
        // Guard against spreadsheet formula injection when the report is opened in Excel.
        boolean formulaLike = !value.isEmpty() && "=+-@".indexOf(value.charAt(0)) >= 0;
        String v = formulaLike ? "'" + value : value;
        if (v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r")) {
            return "\"" + v.replace("\"", "\"\"") + "\"";
        }
        return v;
    }

    private static String abbreviate(String text) {
        String flat = text.replaceAll("\\s+", " ").trim();
        return flat.length() > 60 ? flat.substring(0, 57) + "..." : flat;
    }

    private static TreeSet<String> otherTermIds(String targetTermId, List<TermMatchResult> results) {
        TreeSet<String> ids = new TreeSet<>();
        for (TermMatchResult r : results) {
            if (!targetTermId.equals(r.getTermId())) {
                ids.add(r.getTermId());
            }
        }
        return ids;
    }
}
