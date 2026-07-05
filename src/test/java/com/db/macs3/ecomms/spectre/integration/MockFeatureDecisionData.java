package com.db.macs3.ecomms.spectre.integration;

import com.db.macs3.ecomms.spectre.model.FeatureDecisionRow;

import java.util.List;

/**
 * Builds {@link FeatureDecisionRow} lists that simulate exactly what
 * {@link com.db.macs3.ecomms.spectre.reader.BigQueryViewReader} would produce
 * for a given scenario — satisfying the requirement that integration tests
 * "mock data from view on both input BQ tables" without needing a live
 * BigQuery connection anywhere in the test run.
 *
 * <p>Each factory method here corresponds to one message in
 * {@code fixtures/messages/scenario1.json} and documents which decision-tree
 * path it is designed to exercise.
 */
public final class MockFeatureDecisionData {

    private MockFeatureDecisionData() {}

    private static final String PROCESS_ID  = "test-process-1";
    private static final String PIPELINE_ID = "test-pipeline-1";
    private static final String RUN_DATE    = "20260713";
    private static final String SENT_DATE   = "20260713";

    /**
     * msg-201: one direct STANDARD lexicon feature, "lexicon_test_alpha".
     * Exercises the plain "no noise reduction at all" path — always processed.
     */
    public static List<FeatureDecisionRow> forMsg201() {
        return List.of(
            FeatureDecisionRow.of("msg-201", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "unrestricted",
                    "201", "lexicon", "lexicon_test_alpha", null, "N",
                    "lexicon_test_alpha", false, "{}")
        );
    }

    /**
     * msg-202: a noise-reduction COMPOSITE (operator=OR) with two lexicon
     * sub-features (lexicon_nr_1, lexicon_nr_2), PLUS a standard lexicon
     * feature (lexicon_test_beta). Exercises: NR OR any-match -> skip standard.
     * lexicon_nr_2's pattern ("urgent") matches the message body, so the
     * composite should be considered matched and lexicon_test_beta must NOT
     * be scanned/reported despite its own pattern also technically matching.
     */
    public static List<FeatureDecisionRow> forMsg202() {
        return List.of(
            FeatureDecisionRow.of("msg-202", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "unrestricted",
                    "202", "composite", "composite_nr_test", "OR", "Y",
                    "lexicon_nr_1", true, "{}"),
            FeatureDecisionRow.of("msg-202", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "unrestricted",
                    "202", "composite", "composite_nr_test", "OR", "Y",
                    "lexicon_nr_2", true, "{}"),
            FeatureDecisionRow.of("msg-202", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "unrestricted",
                    "203", "lexicon", "lexicon_test_beta", null, "N",
                    "lexicon_test_beta", false, "{}")
        );
    }

    /**
     * msg-203: RESTRICTED message, direct lexicon feature "lexicon_test_alpha"
     * (same pattern as msg-201). Exercises restricted-message masking: the
     * body AND the attachment both contain "bomb", so this also verifies
     * multi-segment ("msg" + "attachment-0") matched_text JSON construction.
     */
    public static List<FeatureDecisionRow> forMsg203() {
        return List.of(
            FeatureDecisionRow.of("msg-203", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "restricted",
                    "201", "lexicon", "lexicon_test_alpha", null, "N",
                    "lexicon_test_alpha", false, "{}")
        );
    }

    /**
     * msg-204: same direct lexicon feature as msg-201, but the message body
     * contains no matching text at all. Exercises the "zero hits" path:
     * feature-hit-summary should show hit_status=No, and evaluated_lexicons
     * should be empty (no entry at all) in lexicon-hit-summary.
     */
    public static List<FeatureDecisionRow> forMsg204() {
        return List.of(
            FeatureDecisionRow.of("msg-204", RUN_DATE, PROCESS_ID, PIPELINE_ID, SENT_DATE, "unrestricted",
                    "201", "lexicon", "lexicon_test_alpha", null, "N",
                    "lexicon_test_alpha", false, "{}")
        );
    }

    /** @return all mocked feature decisions across every scenario message, as one combined view. */
    public static List<FeatureDecisionRow> allScenarios() {
        return java.util.stream.Stream.of(forMsg201(), forMsg202(), forMsg203(), forMsg204())
                .flatMap(List::stream)
                .collect(java.util.stream.Collectors.toList());
    }
}
