package com.db.macs3.ecomms.spectre.output;

import com.db.macs3.ecomms.spectre.decision.DecisionTreeEvaluator;
import com.db.macs3.ecomms.spectre.decision.FeatureGroupingService;
import com.db.macs3.ecomms.spectre.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutputRowBuilder")
class OutputRowBuilderTest {

    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");
    private static final LocalDate DATASET_PARTITION_VALUE = LocalDate.parse("2026-08-16");

    private static FeatureDecisionRow row(String featureId, String featureType, String featuresToApply, String defJson) {
        return new FeatureDecisionRow("proc-1", "msg-101", DATASET_PARTITION_VALUE, "Lexicon-Tagging",
                featureType, Long.parseLong(featureId), featureId + "-name", null, featuresToApply,
                false, null, defJson, DATASET_PARTITION_VALUE, "101");
    }

    private static String defJson(String feature, int totalTerms, int minHits) {
        return "{\"featureId\":\"1\",\"featureName\":\"x\",\"featureType\":\"Lexicon\",\"isNoiseReduction\":false,"
                + "\"body\":{\"id\":1,\"lexiconName\":\"" + feature + "\",\"objectId\":1,\"totalTermsCount\":" + totalTerms
                + ",\"minimumHits\":" + minHits + ",\"scope\":[\"Message Body\"]}}";
    }

    /**
     * Builds a realistic disclaimer(suppresses one match) + lexicon(one survives) evaluation.
     */
    private static MessageEvaluationResult buildRealisticEvaluation() {
        List<FeatureDecisionRow> rows = List.of(
                row("2", "disclaimer", "std_disclaimer-1", defJson("std_disclaimer-1", 5, 1)),
                row("1", "lexicon", "lexicon_market_cond-1", defJson("lexicon_market_cond-1", 10, 3))
        );
        List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);

        Map<String, List<TermMatchResult>> canned = new HashMap<>();
        canned.put("std_disclaimer-1", List.of(new TermMatchResult("std_disclaimer-1::1", "confidential",
                List.of(AreaMatch.messageBody(new MatchSpan(10, 34, "confidential information"))))));
        canned.put("lexicon_market_cond-1", List.of(
                new TermMatchResult("lexicon_market_cond-1::1", "information",
                        List.of(AreaMatch.messageBody(new MatchSpan(15, 27, "information")))), // suppressed
                new TermMatchResult("lexicon_market_cond-1::2", "bomb",
                        List.of(AreaMatch.messageBody(new MatchSpan(50, 54, "bomb"))))));       // survives

        DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.getFeaturesToApply(), List.of());
        return DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);
    }

    @Nested
    @DisplayName("lexicon-hit-summary")
    class SummaryRow {

        @Test
        @DisplayName("has one evaluated_lexicons entry per evaluated group (disclaimer + lexicon)")
        void hasOneEntryPerGroup() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.getEvaluatedLexicons()).hasSize(2);
        }

        @Test
        @DisplayName("aggregates totalTermsCount and regexHitCount for the disclaimer group, and populates " +
                "regexMatchHitCount as the raw per-term match occurrence count")
        void aggregatesDisclaimerCounts() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            var disclaimerEntry = row.getEvaluatedLexicons().stream()
                    .filter(e -> e.getId().equals("2")).findFirst().orElseThrow();
            assertThat(disclaimerEntry.getTotalTermsCount()).isEqualTo(5);
            assertThat(disclaimerEntry.getRegexHitCount()).isEqualTo(1);
            assertThat(disclaimerEntry.getTermDtls().getFirst().getRegexMatchHitCount()).isEqualTo(1L);
        }

        @Test
        @DisplayName("uses RAW (pre-suppression) counts — summary reflects everything checked, not just alerts")
        void usesPreSuppressionCounts() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            var lexiconEntry = row.getEvaluatedLexicons().stream()
                    .filter(e -> e.getId().equals("1")).findFirst().orElseThrow();
            assertThat(lexiconEntry.getRegexHitCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("omits a group that was evaluated but matched nothing — regex_hit_count would be zero")
        void omitsZeroHitGroups() {
            List<FeatureDecisionRow> rows = List.of(
                    row("1", "lexicon", "lexicon_market_cond-3", defJson("lexicon_market_cond-3", 8, 2)));
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);
            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> List.of(); // no matches at all
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-999", groups, scanner);

            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-999", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            assertThat(row.getEvaluatedLexicons()).isEmpty();
        }

        @Test
        @DisplayName("regexMatchHitCount counts every individual occurrence of a term, not just whether it matched")
        void regexMatchHitCountReflectsMultipleOccurrences() {
            List<FeatureDecisionRow> rows = List.of(
                    row("1", "lexicon", "lexicon_market_cond-2", defJson("lexicon_market_cond-2", 20, 3)));
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);

            // Simulate a pattern matching 5 separate times in the message body.
            List<AreaMatch> fiveOccurrences = List.of(
                    AreaMatch.messageBody(new MatchSpan(0, 6, "market")),
                    AreaMatch.messageBody(new MatchSpan(20, 26, "market")),
                    AreaMatch.messageBody(new MatchSpan(40, 51, "manipulate")),
                    AreaMatch.messageBody(new MatchSpan(60, 66, "market")),
                    AreaMatch.messageBody(new MatchSpan(80, 91, "manipulate")));
            Map<String, List<TermMatchResult>> canned = Map.of("lexicon_market_cond-2", List.of(
                    new TermMatchResult("lexicon_market_cond-2::1", "(?:(?:market|manipulate)", fiveOccurrences)));

            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.getFeaturesToApply(), List.of());
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);

            LexiconHitSummaryRow summaryRow = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            var termDtl = summaryRow.getEvaluatedLexicons().getFirst().getTermDtls().getFirst();
            assertThat(termDtl.getTermId()).isEqualTo("lexicon_market_cond-2::1");
            assertThat(termDtl.getRegexMatchHitCount()).isEqualTo(5L);
        }
    }

    @Nested
    @DisplayName("lexicon-hit-restricted / -unrestricted (detail row)")
    class DetailRow {

        @Test
        @DisplayName("only includes groups with surviving (post-suppression) matches")
        void onlyIncludesSurvivingGroups() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row).isNotNull();
            assertThat(row.getEvaluatedLexicons()).hasSize(1); // disclaimer group excluded entirely
        }

        @Test
        @DisplayName("the suppressed term is gone; only the surviving term remains")
        void suppressedTermIsGone() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.getEvaluatedLexicons().getFirst().getTermDtls()).hasSize(1);
            assertThat(row.getEvaluatedLexicons().getFirst().getTermDtls().getFirst().getTermId())
                    .isEqualTo("lexicon_market_cond-1::2");
        }

        @Test
        @DisplayName("matched_text JSON contains the hit_details_hs wrapper with correct text/position")
        void matchedTextJsonIsCorrect() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, buildRealisticEvaluation(), "scan-engine", NOW);
            String matchedTextJson = row.getEvaluatedLexicons().getFirst().getTermDtls().getFirst().getMatchedText();
            assertThat(matchedTextJson).contains("hit_details_hs");
            assertThat(matchedTextJson).contains("\"bomb\"");
            assertThat(matchedTextJson).contains("\"start\":50");
        }

        @Test
        @DisplayName("returns null for a short-circuited message — nothing to write, not an empty row")
        void returnsNullWhenShortCircuited() {
            List<FeatureDecisionRow> nrRows = List.of(
                    new FeatureDecisionRow("proc-1", "msg-102", DATASET_PARTITION_VALUE, "Lexicon-Tagging",
                            "NoiseReduction", 9L, "9-name", null, "spam-1",
                            true, null, defJson("spam-1", 3, 1), DATASET_PARTITION_VALUE, "101"),
                    row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1))
            );
            List<FeatureGroup> nrGroups = FeatureGroupingService.groupAndOrder(nrRows);
            Map<String, List<TermMatchResult>> nrCanned = Map.of(
                    "spam-1", List.of(new TermMatchResult("spam-1::1", "spam",
                            List.of(AreaMatch.messageBody(new MatchSpan(0, 4, "spam"))))));
            MessageEvaluationResult nrEval = DecisionTreeEvaluator.evaluate("msg-102", nrGroups,
                    r -> nrCanned.getOrDefault(r.getFeaturesToApply(), List.of()));

            assertThat(nrEval.isShortCircuited()).isTrue(); // sanity check the premise before checking the row builder

            LexiconHitDetailRow nullRow = OutputRowBuilder.buildDetailRow(
                    "msg-102", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, nrEval, "scan-engine", NOW);
            assertThat(nullRow).isNull();
        }
    }

    @Nested
    @DisplayName("feature-hit-summary")
    class FeatureHitSummary {

        @Test
        @DisplayName("has one entry per evaluated group with correctly resolved hit status")
        void hasCorrectHitStatus() {
            FeatureHitSummaryRow row = OutputRowBuilder.buildFeatureHitSummaryRow(
                    "msg-101", DATASET_PARTITION_VALUE, "pipe-1", "proc-1", "Lexicon-Tagging",
                    buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.getFeatures()).hasSize(2);
            var disclaimerFeature = row.getFeatures().stream().filter(f -> f.getId() == 2L).findFirst().orElseThrow();
            assertThat(disclaimerFeature.getHitStatus()).isTrue();
            assertThat(disclaimerFeature.getSubFeatures()).isEmpty(); // single-member group
        }
    }
}
