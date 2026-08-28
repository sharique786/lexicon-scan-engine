package com.db.macs3.ecomms.spectre.scanengine.output;

import com.db.macs3.ecomms.spectre.scanengine.decision.DecisionTreeEvaluator;
import com.db.macs3.ecomms.spectre.scanengine.decision.FeatureGroupingService;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.output.*;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutputRowBuilder")
class OutputRowBuilderTest {

    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");

    private static FeatureDecisionRow row(String featureId, String featureType, String featuresToApply, String defJson) {
        return new FeatureDecisionRow("proc-1", "msg-101", "part-1", "Lexicon-Tagging",
                featureType, featureId, featureId + "-name", null, featuresToApply,
                "N", null, defJson, "2026-08-16", "101");
    }

    private static String defJson(String feature, int totalTerms, int minHits) {
        return "{\"featureName\":\"x\",\"featureType\":\"Lexicon\",\"isNoiseReduction\":false,"
                + "\"body\":{\"feature\":\"" + feature + "\",\"totalTermsCount\":" + totalTerms
                + ",\"minimumHits\":" + minHits + ",\"scope\":[\"Message Body\"]}}";
    }

    /** Builds a realistic disclaimer(suppresses one match) + lexicon(one survives) evaluation. */
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

        DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
        return DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);
    }

    @Nested
    @DisplayName("lexicon-hit-summary")
    class SummaryRow {

        @Test
        @DisplayName("has one evaluated_lexicons entry per evaluated group (disclaimer + lexicon)")
        void hasOneEntryPerGroup() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.evaluatedLexicons()).hasSize(2);
        }

        @Test
        @DisplayName("aggregates totalTermsCount and regexHitCount for the disclaimer group, and populates " +
                     "regexMatchHitCount as the raw per-term match occurrence count")
        void aggregatesDisclaimerCounts() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", buildRealisticEvaluation(), "scan-engine", NOW);
            var disclaimerEntry = row.evaluatedLexicons().stream()
                    .filter(e -> e.id().equals("2")).findFirst().orElseThrow();
            assertThat(disclaimerEntry.totalTermsCount()).isEqualTo(5);
            assertThat(disclaimerEntry.regexHitCount()).isEqualTo(1);
            assertThat(disclaimerEntry.termDtls().get(0).regexMatchHitCount()).isEqualTo(1L);
        }

        @Test
        @DisplayName("uses RAW (pre-suppression) counts — summary reflects everything checked, not just alerts")
        void usesPreSuppressionCounts() {
            LexiconHitSummaryRow row = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", buildRealisticEvaluation(), "scan-engine", NOW);
            var lexiconEntry = row.evaluatedLexicons().stream()
                    .filter(e -> e.id().equals("1")).findFirst().orElseThrow();
            assertThat(lexiconEntry.regexHitCount()).isEqualTo(2);
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

            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);

            LexiconHitSummaryRow summaryRow = OutputRowBuilder.buildSummaryRow(
                    "msg-101", "proc-1", "pipe-1", evaluation, "scan-engine", NOW);

            var termDtl = summaryRow.evaluatedLexicons().get(0).termDtls().get(0);
            assertThat(termDtl.termId()).isEqualTo("lexicon_market_cond-2::1");
            assertThat(termDtl.regexMatchHitCount()).isEqualTo(5L);
        }
    }

    @Nested
    @DisplayName("lexicon-hit-restricted / -unrestricted (detail row)")
    class DetailRow {

        @Test
        @DisplayName("only includes groups with surviving (post-suppression) matches")
        void onlyIncludesSurvivingGroups() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", "partition-1", buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row).isNotNull();
            assertThat(row.evaluatedLexicons()).hasSize(1); // disclaimer group excluded entirely
        }

        @Test
        @DisplayName("the suppressed term is gone; only the surviving term remains")
        void suppressedTermIsGone() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", "partition-1", buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.evaluatedLexicons().get(0).termDtls()).hasSize(1);
            assertThat(row.evaluatedLexicons().get(0).termDtls().get(0).termId())
                    .isEqualTo("lexicon_market_cond-1::2");
        }

        @Test
        @DisplayName("matched_text JSON contains the hit_details_hs wrapper with correct text/position")
        void matchedTextJsonIsCorrect() {
            LexiconHitDetailRow row = OutputRowBuilder.buildDetailRow(
                    "msg-101", "proc-1", "pipe-1", "partition-1", buildRealisticEvaluation(), "scan-engine", NOW);
            String matchedTextJson = row.evaluatedLexicons().get(0).termDtls().get(0).matchedText();
            assertThat(matchedTextJson).contains("hit_details_hs");
            assertThat(matchedTextJson).contains("\"bomb\"");
            assertThat(matchedTextJson).contains("\"start\":50");
        }

        @Test
        @DisplayName("returns null for a short-circuited message — nothing to write, not an empty row")
        void returnsNullWhenShortCircuited() {
            List<FeatureDecisionRow> nrRows = List.of(
                    new FeatureDecisionRow("proc-1", "msg-102", "part-1", "Lexicon-Tagging",
                            "NoiseReduction", "9", "9-name", null, "spam-1",
                            "Y", null, defJson("spam-1", 3, 1), "2026-08-16", "101"),
                    row("1", "lexicon", "lex-1", defJson("lex-1", 3, 1))
            );
            List<FeatureGroup> nrGroups = FeatureGroupingService.groupAndOrder(nrRows);
            Map<String, List<TermMatchResult>> nrCanned = Map.of(
                    "spam-1", List.of(new TermMatchResult("spam-1::1", "spam",
                            List.of(AreaMatch.messageBody(new MatchSpan(0, 4, "spam"))))));
            MessageEvaluationResult nrEval = DecisionTreeEvaluator.evaluate("msg-102", nrGroups,
                    r -> nrCanned.getOrDefault(r.featuresToApply(), List.of()));

            assertThat(nrEval.shortCircuited()).isTrue(); // sanity check the premise before checking the row builder

            LexiconHitDetailRow nullRow = OutputRowBuilder.buildDetailRow(
                    "msg-102", "proc-1", "pipe-1", "partition-1", nrEval, "scan-engine", NOW);
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
                    "msg-101", "partition-1", "pipe-1", "proc-1", "Lexicon-Tagging",
                    buildRealisticEvaluation(), "scan-engine", NOW);
            assertThat(row.features()).hasSize(2);
            var disclaimerFeature = row.features().stream().filter(f -> f.id() == 2L).findFirst().orElseThrow();
            assertThat(disclaimerFeature.hitStatus()).isTrue();
            assertThat(disclaimerFeature.subFeatures()).isEmpty(); // single-member group
        }
    }
}
