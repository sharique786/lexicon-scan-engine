package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.model.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.model.TermMatch;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("NoiseReductionEvaluator Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NoiseReductionEvaluatorTest {

    private NoiseReductionEvaluator evaluator;

    @BeforeEach
    void setUp() {
        evaluator = new NoiseReductionEvaluator();
    }

    private FeatureDecisionRow nrRow(String featureId, String lexiconName, String operator) {
        return FeatureDecisionRow.of("msg-1", "20260713", "proc-1", "pipe-1", "20260713", "unrestricted",
                featureId, "lexicon", "parentName", operator, "Y", lexiconName, false, "{}");
    }

    private Map<String, List<TermMatch>> hits(String... lexiconNames) {
        Map<String, List<TermMatch>> results = new HashMap<>();
        for (String name : lexiconNames) {
            results.put(name, List.of(TermMatch.of(0, name + "::1", "pattern", "bomb", 14, 17, 0)));
        }
        return results;
    }

    private Map<String, List<TermMatch>> noHits(String... lexiconNames) {
        Map<String, List<TermMatch>> results = new HashMap<>();
        for (String name : lexiconNames) results.put(name, Collections.emptyList());
        return results;
    }

    @Test @Order(1)
    @DisplayName("No NR features -> never skip standard")
    void noNrFeatures_neverSkip() {
        assertThat(evaluator.shouldSkipStandardFeatures(Collections.emptyList(), Map.of())).isFalse();
        assertThat(evaluator.shouldSkipStandardFeatures(null, Map.of())).isFalse();
    }

    @Test @Order(10)
    @DisplayName("NR OR — ANY feature matches -> skip standard")
    void nrOr_anyMatch_skip() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", "OR"), nrRow("2", "lex_b", "OR"));
        Map<String, List<TermMatch>> results = hits("lex_a");
        results.put("lex_b", Collections.emptyList());
        assertThat(evaluator.shouldSkipStandardFeatures(nr, results)).isTrue();
    }

    @Test @Order(11)
    @DisplayName("NR OR — NONE match -> process standard")
    void nrOr_noneMatch_doNotSkip() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", "OR"), nrRow("2", "lex_b", "OR"));
        assertThat(evaluator.shouldSkipStandardFeatures(nr, noHits("lex_a", "lex_b"))).isFalse();
    }

    @Test @Order(20)
    @DisplayName("NR AND — ALL features match -> skip standard")
    void nrAnd_allMatch_skip() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", "AND"), nrRow("2", "lex_b", "AND"));
        assertThat(evaluator.shouldSkipStandardFeatures(nr, hits("lex_a", "lex_b"))).isTrue();
    }

    @Test @Order(21)
    @DisplayName("NR AND — PARTIAL match -> process standard")
    void nrAnd_partialMatch_doNotSkip() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", "AND"), nrRow("2", "lex_b", "AND"));
        Map<String, List<TermMatch>> results = hits("lex_a");
        results.put("lex_b", Collections.emptyList());
        assertThat(evaluator.shouldSkipStandardFeatures(nr, results)).isFalse();
    }

    @Test @Order(30)
    @DisplayName("Composite with multiple lexicon sub-features under ONE parent id: " +
                 "ANY sub-feature hit counts as the whole parent feature matching")
    void compositeSubFeaturesGroupedByParent_anyHitCountsAsParentMatch() {
        // Same parent featureId=1 (a composite), two lexicon sub-features.
        // Only sub_2 matches -> the PARENT is considered matched (OR within composite),
        // and since there's only one parent feature, NR OR/AND both skip standard.
        List<FeatureDecisionRow> nr = List.of(
                nrRow("1", "sub_1", "OR"),
                nrRow("1", "sub_2", "OR")
        );
        Map<String, List<TermMatch>> results = hits("sub_2");
        results.put("sub_1", Collections.emptyList());
        assertThat(evaluator.shouldSkipStandardFeatures(nr, results)).isTrue();
    }

    @Test @Order(31)
    @DisplayName("Two DIFFERENT parent composites, AND operator: both parents must match")
    void twoParentComposites_andOperator_bothMustMatch() {
        // Parent 1 (composite A) has one matching sub-feature -> parent 1 matched
        // Parent 2 (composite B) has no matching sub-feature -> parent 2 NOT matched
        // AND requires ALL parents matched -> should NOT skip
        List<FeatureDecisionRow> nr = List.of(
                nrRow("1", "a_sub1", "AND"),
                nrRow("2", "b_sub1", "AND")
        );
        Map<String, List<TermMatch>> results = hits("a_sub1");
        results.put("b_sub1", Collections.emptyList());
        assertThat(evaluator.shouldSkipStandardFeatures(nr, results)).isFalse();
    }

    @Test @Order(40)
    @DisplayName("resolveOperator defaults to OR when operator is null/blank")
    void resolveOperator_defaultsToOr() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", null), nrRow("2", "lex_b", ""));
        assertThat(evaluator.resolveOperator(nr)).isEqualTo("OR");
    }

    @Test @Order(41)
    @DisplayName("resolveOperator is case-insensitive and normalises to uppercase")
    void resolveOperator_caseInsensitive() {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "lex_a", "and"));
        assertThat(evaluator.resolveOperator(nr)).isEqualTo("AND");
    }

    @Test @Order(50)
    @DisplayName("featureHasMatch returns false for lexicon not present in results map")
    void featureHasMatch_missingFromMap() {
        assertThat(evaluator.featureHasMatch("missing", Map.of())).isFalse();
    }

    @ParameterizedTest(name = "[{index}] op={0} allMatch={1} anyMatch={2} -> skip={3}")
    @CsvSource({
        "OR,  true,  true,  true",
        "OR,  false, true,  true",
        "OR,  false, false, false",
        "AND, true,  true,  true",
        "AND, false, true,  false",
        "AND, false, false, false"
    })
    @Order(60)
    void decisionMatrixParameterized(String operator, boolean allMatch, boolean anyMatch, boolean expectedSkip) {
        List<FeatureDecisionRow> nr = List.of(nrRow("1", "f1", operator), nrRow("2", "f2", operator));
        Map<String, List<TermMatch>> results;
        if (allMatch) {
            results = hits("f1", "f2");
        } else if (anyMatch) {
            results = hits("f1");
            results.put("f2", Collections.emptyList());
        } else {
            results = noHits("f1", "f2");
        }
        assertThat(evaluator.shouldSkipStandardFeatures(nr, results)).isEqualTo(expectedSkip);
    }
}
