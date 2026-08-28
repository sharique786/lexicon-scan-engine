package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DecisionTreeEvaluator")
class DecisionTreeEvaluatorTest {

    private static FeatureDecisionRow row(String featureId, String featureType, String featuresToApply,
                                           String isNoiseReduction, String operator) {
        return new FeatureDecisionRow("proc-1", "msg-101", "part-1", "Lexicon-Tagging",
                featureType, featureId, featureId + "-name", "lexicon", featuresToApply,
                isNoiseReduction, operator, "{}", "2026-08-16", "101");
    }

    private static TermMatchResult oneMatch(String termId, MatchArea area, int start, int end, String text) {
        return new TermMatchResult(termId, "pattern-" + termId,
                List.of(new AreaMatch(area, area == MatchArea.ATTACHMENT ? "att-1" : null,
                        new MatchSpan(start, end, text))));
    }

    private static List<TermMatchResult> flattenAll(MessageEvaluationResult r) {
        List<TermMatchResult> all = new ArrayList<>();
        for (var v : r.finalLexiconMatchesByFeatureId().values()) all.addAll(v);
        return all;
    }

    @Nested
    @DisplayName("noise-reduction short-circuit")
    class NoiseReductionShortCircuit {

        @Test
        @DisplayName("an OR group with any member hit short-circuits — no later group is even scanned")
        void orGroupHitShortCircuits() {
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                    row("3", "composite", "lex_a", "Y", "OR"),
                    row("3", "composite", "lex_b", "Y", "OR"),
                    row("2", "disclaimer", "disc_1", "N", null),
                    row("1", "lexicon", "lex_std", "N", null)
            ));
            Map<String, List<TermMatchResult>> canned = new HashMap<>();
            canned.put("lex_a", List.of());
            canned.put("lex_b", List.of(oneMatch("lex_b::1", MatchArea.MESSAGE_BODY, 0, 4, "spam")));

            List<String> scannedFeatures = new ArrayList<>();
            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> {
                scannedFeatures.add(r.featuresToApply());
                return canned.getOrDefault(r.featuresToApply(), List.of());
            };

            MessageEvaluationResult result = DecisionTreeEvaluator.evaluate("msg-101", groups, scanner);

            assertThat(result.shortCircuited()).isTrue();
            assertThat(flattenAll(result)).isEmpty();
            assertThat(scannedFeatures).doesNotContain("disc_1", "lex_std");
            assertThat(result.evaluatedGroups()).hasSize(1);
        }

        @Test
        @DisplayName("an AND group with only SOME members hit does NOT short-circuit — processing continues")
        void andGroupPartialHitProceeds() {
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                    row("5", "NoiseReduction", "spam_1", "Y", "AND"),
                    row("5", "NoiseReduction", "spam_2", "Y", "AND"),
                    row("1", "lexicon", "lex_std", "N", null)
            ));
            Map<String, List<TermMatchResult>> canned = new HashMap<>();
            canned.put("spam_1", List.of(oneMatch("spam_1::1", MatchArea.MESSAGE_BODY, 0, 4, "spam")));
            canned.put("spam_2", List.of()); // AND requires BOTH -- group is not a hit
            canned.put("lex_std", List.of(oneMatch("lex_std::1", MatchArea.MESSAGE_BODY, 10, 14, "bomb")));

            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
            MessageEvaluationResult result = DecisionTreeEvaluator.evaluate("msg-102", groups, scanner);

            assertThat(result.shortCircuited()).isFalse();
            assertThat(flattenAll(result)).hasSize(1);
        }
    }

    @Nested
    @DisplayName("disclaimer-overlap suppression (full containment only)")
    class DisclaimerSuppression {

        @Test
        @DisplayName("a fully-contained match is suppressed; a partially-overlapping one is NOT")
        void fullContainmentOnly() {
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                    row("2", "disclaimer", "disc_1", "N", null),
                    row("1", "lexicon", "lex_std", "N", null)
            ));
            Map<String, List<TermMatchResult>> canned = new HashMap<>();
            canned.put("disc_1", List.of(oneMatch("disc_1::1", MatchArea.MESSAGE_BODY, 10, 34, "confidential information")));
            canned.put("lex_std", List.of(
                    new TermMatchResult("lex_std::1", "p1", List.of(
                            new AreaMatch(MatchArea.MESSAGE_BODY, null, new MatchSpan(15, 27, "information")))),
                    new TermMatchResult("lex_std::2", "p2", List.of(
                            new AreaMatch(MatchArea.MESSAGE_BODY, null, new MatchSpan(30, 40, "partial"))))));

            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
            MessageEvaluationResult result = DecisionTreeEvaluator.evaluate("msg-103", groups, scanner);

            assertThat(flattenAll(result)).hasSize(1);
            assertThat(flattenAll(result).get(0).termId()).isEqualTo("lex_std::2");
            assertThat(result.suppressedLexiconMatchCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("a disclaimer match in SUBJECT does not suppress a lexicon match at the same " +
                     "numeric indices in MESSAGE_BODY — different coordinate spaces")
        void doesNotCrossAreaBoundaries() {
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                    row("2", "disclaimer", "disc_1", "N", null),
                    row("1", "lexicon", "lex_std", "N", null)
            ));
            Map<String, List<TermMatchResult>> canned = new HashMap<>();
            canned.put("disc_1", List.of(oneMatch("disc_1::1", MatchArea.SUBJECT, 0, 10, "disclaimer")));
            canned.put("lex_std", List.of(oneMatch("lex_std::1", MatchArea.MESSAGE_BODY, 0, 10, "disclaimer")));

            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
            MessageEvaluationResult result = DecisionTreeEvaluator.evaluate("msg-104", groups, scanner);

            assertThat(flattenAll(result)).hasSize(1);
        }

        @Test
        @DisplayName("finalLexiconMatchesByFeatureId preserves per-group structure, needed to rebuild " +
                     "which evaluated group each surviving match belongs to")
        void preservesPerGroupStructure() {
            List<FeatureGroup> groups = FeatureGroupingService.groupAndOrder(List.of(
                    row("1", "lexicon", "lex_a", "N", null),
                    row("4", "lexicon", "lex_b", "N", null)
            ));
            Map<String, List<TermMatchResult>> canned = Map.of(
                    "lex_a", List.of(oneMatch("lex_a::1", MatchArea.MESSAGE_BODY, 0, 4, "bomb")),
                    "lex_b", List.of(oneMatch("lex_b::1", MatchArea.MESSAGE_BODY, 10, 14, "riot")));
            DecisionTreeEvaluator.FeatureRowScanner scanner = r -> canned.getOrDefault(r.featuresToApply(), List.of());
            MessageEvaluationResult result = DecisionTreeEvaluator.evaluate("msg-105", groups, scanner);

            assertThat(result.finalLexiconMatchesByFeatureId()).containsKeys("1", "4");
            assertThat(result.finalLexiconMatchesByFeatureId().get("1").get(0).termId()).isEqualTo("lex_a::1");
            assertThat(result.finalLexiconMatchesByFeatureId().get("4").get(0).termId()).isEqualTo("lex_b::1");
        }
    }
}
