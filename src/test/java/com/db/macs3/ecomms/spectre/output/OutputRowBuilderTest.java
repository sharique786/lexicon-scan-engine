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
        return "{\"featureId\":\"1\",\"featureName\":\"x\",\"featureType\":\"lexicon\",\"isNoiseReduction\":\"N\","
                + "\"body\":{\"id\":1,\"lexiconName\":\"" + feature + "\",\"objectId\":\"1\",\"totalTermsCount\":" + totalTerms
                + ",\"minimumHits\":" + minHits + ",\"scope\":[\"Message Body\"]}}";
    }

    private static FeatureDecisionRow noiseRow(long featureId, String featuresToApply, String operator) {
        return new FeatureDecisionRow("proc-1", "msg-102", DATASET_PARTITION_VALUE, "Lexicon-Tagging",
                "NoiseReduction", featureId, featureId + "-name", null, featuresToApply,
                true, operator, defJson(featuresToApply, 3, 1), DATASET_PARTITION_VALUE, "101");
    }

    /** A NoiseReduction group that hits, ahead of a lexicon group that is therefore never evaluated. */
    private static MessageEvaluationResult shortCircuitedEvaluation() {
        List<FeatureDecisionRow> rows = List.of(
                noiseRow(9L, "spam-1", null), row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1)));
        Map<String, List<TermMatchResult>> canned = Map.of(
                "spam-1", List.of(new TermMatchResult("spam-1::1", "spam",
                        List.of(AreaMatch.messageBody(new MatchSpan(0, 4, "spam"))))));
        return DecisionTreeEvaluator.evaluate("msg-102", FeatureGroupingService.groupAndOrder(rows),
                r -> canned.getOrDefault(r.getFeaturesToApply(), List.of()));
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
                    .filter(e -> e.getId().equals(2L)).findFirst().orElseThrow();
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
                    .filter(e -> e.getId().equals(1L)).findFirst().orElseThrow();
            assertThat(lexiconEntry.getRegexHitCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("a group that was evaluated but matched nothing is still reported: real id/name/" +
                "total_terms_count, regex_hit_count 0, and one N/A / N/A / 0 term_dtls placeholder")
        void reportsZeroHitGroupWithPlaceholderTerm() {
            List<FeatureDecisionRow> rows = List.of(
                    row("1", "lexicon", "lexicon_market_cond-3", defJson("lexicon_market_cond-3", 8, 2)));
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);
            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> List.of(); // no matches at all
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-999", groups, scanner);

            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-999", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            assertThat(row.getEvaluatedLexicons()).hasSize(1);
            var entry = row.getEvaluatedLexicons().getFirst();
            assertThat(entry.getId()).isEqualTo(1L);
            assertThat(entry.getName()).isEqualTo("1-name");
            assertThat(entry.getTotalTermsCount()).isEqualTo(8L);
            assertThat(entry.getRegexHitCount()).isZero();
            assertThat(entry.getTermDtls()).hasSize(1);
            assertThat(entry.getTermDtls().getFirst().getTermId()).isEqualTo("N/A");
            assertThat(entry.getTermDtls().getFirst().getTermRegexPattern()).isEqualTo("N/A");
            assertThat(entry.getTermDtls().getFirst().getRegexMatchHitCount()).isZero();
        }

        @Test
        @DisplayName("in one message, a group that hit keeps its real terms while a group that did not " +
                "gets the placeholder — neither affects the other")
        void mixedHitAndNoHitGroups() {
            List<FeatureDecisionRow> rows = List.of(
                    row("2", "disclaimer", "std_disclaimer-1", defJson("std_disclaimer-1", 5, 1)),
                    row("1", "lexicon", "lexicon_market_cond-1", defJson("lexicon_market_cond-1", 10, 3)));
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(rows);
            Map<String, List<TermMatchResult>> canned = Map.of("lexicon_market_cond-1", List.of(
                    new TermMatchResult("lexicon_market_cond-1::2", "bomb",
                            List.of(AreaMatch.messageBody(new MatchSpan(50, 54, "bomb"))))));
            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.getFeaturesToApply(), List.of());
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);

            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            var disclaimer = row.getEvaluatedLexicons().stream().filter(e -> e.getId().equals(2L)).findFirst().orElseThrow();
            assertThat(disclaimer.getRegexHitCount()).isZero();
            assertThat(disclaimer.getTermDtls().getFirst().getTermId()).isEqualTo("N/A");
            var lexicon = row.getEvaluatedLexicons().stream().filter(e -> e.getId().equals(1L)).findFirst().orElseThrow();
            assertThat(lexicon.getRegexHitCount()).isEqualTo(1L);
            assertThat(lexicon.getTermDtls()).hasSize(1);
            assertThat(lexicon.getTermDtls().getFirst().getTermId()).isEqualTo("lexicon_market_cond-1::2");
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
        @DisplayName("a message short-circuited by a NoiseReduction hit still gets a detail row carrying that " +
                "group's match — id, term_id, and matched_text")
        void noiseReductionHitIsWrittenToDetailRow() {
            MessageEvaluationResult nrEval = shortCircuitedEvaluation();
            assertThat(nrEval.isShortCircuited()).isTrue(); // sanity check the premise before checking the row builder

            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-102", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, nrEval, "scan-engine", NOW);

            assertThat(row).isNotNull();
            assertThat(row.getEvaluatedLexicons()).hasSize(1); // the never-evaluated lexicon group is absent
            var entry = row.getEvaluatedLexicons().getFirst();
            assertThat(entry.getId()).isEqualTo(9L);
            assertThat(entry.getTermDtls()).hasSize(1);
            assertThat(entry.getTermDtls().getFirst().getTermId()).isEqualTo("spam-1::1");
            assertThat(entry.getTermDtls().getFirst().getMatchedText())
                    .contains("hit_details_hs").contains("\"spam\"").contains("\"start\":0");
        }

        @Test
        @DisplayName("the same message's lexicon-hit-summary row carries the NoiseReduction match too, " +
                "so all three tables agree")
        void noiseReductionHitIsInSummaryRowToo() {
            LexiconHitSummaryRow summary = OutputRowBuilder.buildSummaryRow(
                    "msg-102", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, shortCircuitedEvaluation(), "scan-engine", NOW);

            assertThat(summary.getEvaluatedLexicons()).hasSize(1);
            var entry = summary.getEvaluatedLexicons().getFirst();
            assertThat(entry.getId()).isEqualTo(9L);
            assertThat(entry.getRegexHitCount()).isEqualTo(1L);
            assertThat(entry.getTermDtls().getFirst().getTermId()).isEqualTo("spam-1::1");
            assertThat(entry.getTermDtls().getFirst().getRegexMatchHitCount()).isEqualTo(1L);
        }

        @Test
        @DisplayName("every matching member of a hit multi-member NoiseReduction group is written, under its one id")
        void multiMemberNoiseGroupWritesAllMemberMatches() {
            List<FeatureDecisionRow> rows = List.of(
                    noiseRow(9L, "spam-1", "OR"), noiseRow(9L, "spam-2", "OR"),
                    row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1)));
            Map<String, List<TermMatchResult>> canned = Map.of(
                    "spam-1", List.of(new TermMatchResult("spam-1::1", "spam",
                            List.of(AreaMatch.messageBody(new MatchSpan(0, 4, "spam"))))),
                    "spam-2", List.of(new TermMatchResult("spam-2::3", "junk",
                            List.of(AreaMatch.subject(new MatchSpan(2, 6, "junk"))))));
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-102",
                    FeatureGroupingService.groupAndOrder(rows), r -> canned.getOrDefault(r.getFeaturesToApply(), List.of()));

            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-102", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            assertThat(row.getEvaluatedLexicons()).hasSize(1);
            assertThat(row.getEvaluatedLexicons().getFirst().getTermDtls())
                    .extracting(LexiconHitDetailRow.EvaluatedLexicon.TermDtl::getTermId)
                    .containsExactlyInAnyOrder("spam-1::1", "spam-2::3");
        }

        @Test
        @DisplayName("a NoiseReduction group that is NOT a hit (AND group, only one member matched) is not " +
                "written — only the lexicon hit that followed is")
        void nonHitNoiseGroupIsNotWritten() {
            List<FeatureDecisionRow> rows = List.of(
                    noiseRow(9L, "spam-1", "AND"), noiseRow(9L, "spam-2", "AND"),
                    row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1)));
            Map<String, List<TermMatchResult>> canned = Map.of(
                    "spam-1", List.of(new TermMatchResult("spam-1::1", "spam",
                            List.of(AreaMatch.messageBody(new MatchSpan(0, 4, "spam"))))), // spam-2 has none: AND fails
                    "lex-1", List.of(new TermMatchResult("lex-1::1", "bomb",
                            List.of(AreaMatch.messageBody(new MatchSpan(10, 14, "bomb"))))));
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-102",
                    FeatureGroupingService.groupAndOrder(rows), r -> canned.getOrDefault(r.getFeaturesToApply(), List.of()));
            assertThat(evaluation.isShortCircuited()).isFalse();

            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-102", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW);

            assertThat(row.getEvaluatedLexicons()).hasSize(1);
            assertThat(row.getEvaluatedLexicons().getFirst().getId()).isEqualTo(1L);
        }

        @Test
        @DisplayName("still returns null — no row — when nothing hit at all")
        void returnsNullWhenNothingHit() {
            List<FeatureDecisionRow> rows = List.of(
                    noiseRow(9L, "spam-1", null), row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1)));
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-103",
                    FeatureGroupingService.groupAndOrder(rows), r -> List.of());

            assertThat(OutputRowBuilder.buildDetailRow(
                    "msg-103", "proc-1", "pipe-1", DATASET_PARTITION_VALUE, evaluation, "scan-engine", NOW)).isNull();
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
