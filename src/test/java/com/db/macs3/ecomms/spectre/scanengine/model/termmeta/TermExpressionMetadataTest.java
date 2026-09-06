package com.db.macs3.ecomms.spectre.scanengine.model.termmeta;

import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata.TermEntry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TermExpressionMetadata}.
 *
 * <h2>What this class exists to fix — see class Javadoc</h2>
 * <p>Confirmed gap: before the Compile Service's AND NOT fix, a {@code .hdb}
 * file alone was self-sufficient for resolving any matched expression id
 * back to a term identity. The fix removed native COMBINATION for AND NOT
 * terms, meaning an AND NOT term's required/excluded patterns now each get
 * their own allocated id, distinct from the term's own number — resolving
 * these correctly requires this class, parsing the Compile Service's
 * {@code compile-results.json}.
 */
@DisplayName("TermExpressionMetadata")
class TermExpressionMetadataTest {

    private static final String FEATURE = "lex_test-1";

    @Nested
    @DisplayName("Non-AND-NOT terms (hyperscanExpressionId)")
    class NonAndNotTerms {

        @Test
        @DisplayName("A simple term's hyperscanExpressionId becomes its single requiredExpressionIds entry")
        void simpleTermResolvesViaHyperscanExpressionId() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["insider"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(1);

            assertThat(entry).isNotNull();
            assertThat(entry.getTermNumber()).isEqualTo(1);
            assertThat(entry.isRequiresExclusionCheck()).isFalse();
            assertThat(entry.getRequiredExpressionIds()).containsExactly(1);
            assertThat(entry.getExcludedExpressionIds()).isNull();
            assertThat(entry.isNativelyResolved()).isTrue();
        }

        @Test
        @DisplayName("A decomposed term's term_regex_pattern joins translatedPattern entries, readable")
        void decomposedTermJoinsPatternText() {
            String json = """
                {"results": [
                  {"termId": "%s::2", "compilationStatus": "PASS",
                   "translatedPattern": ["alpha", "beta", "gamma"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 2}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(2);

            assertThat(entry.getTermRegexPattern()).contains("alpha").contains("beta").contains("gamma");
            assertThat(entry.getRequiredExpressionIds()).containsExactly(2);
        }

        @Test
        @DisplayName("A FAILED term is not indexed at all — it was never compiled into the .hdb")
        void failedTermsAreNotIndexed() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "FAILED", "translatedPattern": null,
                   "requiresExclusionCheck": false}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            assertThat(meta.termByAnyExpressionId(1)).isNull();
            assertThat(meta.termCount()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("AND NOT terms (requiredExpressionIds / excludedExpressionIds)")
    class AndNotTerms {

        @Test
        @DisplayName("An AND NOT term indexes BOTH its required and excluded ids, back to the SAME term entry")
        void andNotTermIndexesBothSides() {
            String json = """
                {"results": [
                  {"termId": "%s::3", "compilationStatus": "PASS", "translatedPattern": ["insider"],
                   "requiresExclusionCheck": true, "requiredExpressionIds": [5], "excludedExpressionIds": [6]}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry viaRequired = meta.termByAnyExpressionId(5);
            TermEntry viaExcluded = meta.termByAnyExpressionId(6);

            assertThat(viaRequired).isNotNull();
            assertThat(viaExcluded).isNotNull();
            assertThat(viaRequired.getTermNumber()).isEqualTo(viaExcluded.getTermNumber());
            assertThat(viaRequired.getTermNumber()).isEqualTo(3);
            assertThat(viaRequired.isRequiresExclusionCheck()).isTrue();
            assertThat(viaRequired.isNativelyResolved())
                    .as("AND NOT terms are never natively resolved by Hyperscan any more")
                    .isFalse();
        }

        @Test
        @DisplayName("A decomposed required side produces multiple requiredExpressionIds entries")
        void decomposedRequiredSideMultipleIds() {
            String json = """
                {"results": [
                  {"termId": "%s::9", "compilationStatus": "PASS",
                   "translatedPattern": ["alpha", "beta", "gamma"],
                   "requiresExclusionCheck": true, "requiredExpressionIds": [10, 11, 12],
                   "excludedExpressionIds": [13]}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(11); // middle leaf

            assertThat(entry).isNotNull();
            assertThat(entry.getTermNumber()).isEqualTo(9);
            assertThat(entry.getRequiredExpressionIds()).containsExactly(10, 11, 12);
            assertThat(entry.getExcludedExpressionIds()).containsExactly(13);
            // Every leaf id resolves back to the SAME entry.
            assertThat(meta.termByAnyExpressionId(10).getTermNumber()).isEqualTo(9);
            assertThat(meta.termByAnyExpressionId(12).getTermNumber()).isEqualTo(9);
        }
    }

    @Nested
    @DisplayName("Mixed feature: multiple terms of different kinds coexist correctly")
    class MixedFeature {

        @Test
        @DisplayName("A simple term, a decomposed term, and an AND NOT term are all correctly, independently indexed")
        void allTermKindsCoexist() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["simple"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 1},
                  {"termId": "%s::2", "compilationStatus": "PASS", "translatedPattern": ["a", "b"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 2},
                  {"termId": "%s::3", "compilationStatus": "PASS", "translatedPattern": ["req"],
                   "requiresExclusionCheck": true, "requiredExpressionIds": [20], "excludedExpressionIds": [21]}
                ]}
                """.formatted(FEATURE, FEATURE, FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);

            assertThat(meta.termCount()).isEqualTo(3);
            assertThat(meta.termByAnyExpressionId(1).getTermNumber()).isEqualTo(1);
            assertThat(meta.termByAnyExpressionId(2).getTermNumber()).isEqualTo(2);
            assertThat(meta.termByAnyExpressionId(20).getTermNumber()).isEqualTo(3);
            assertThat(meta.termByAnyExpressionId(21).getTermNumber()).isEqualTo(3);
            assertThat(meta.termByAnyExpressionId(999)).isNull();
        }
    }

    @Nested
    @DisplayName("resolvedPatterns terms (NEAR / FOLLOWEDBY, new schema)")
    class ResolvedPatternsTerms {

        @Test
        @DisplayName("A NEAR chain term (sample term 1 shape) parses into a 2-leaf Chain, unchanged requiredExpressionIds")
        void nearChainTermParsesCorrectly() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "(?:price|spread|stock)"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "manipulate NEAR{5} (?:price|spread|stock)",
                   "hyperscanExpressionId": 1, "patternMapping": "(7&8)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(1);

            assertThat(entry).isNotNull();
            assertThat(entry.getTermNumber()).isEqualTo(1);
            assertThat(entry.getRequiredExpressionIds()).containsExactly(1);
            assertThat(entry.requiresPerAreaEvaluation()).isTrue();
            assertThat(entry.isNativelyResolved())
                    .as("a resolvedPatterns term always needs per-area verification, even a plain chain")
                    .isFalse();
            assertThat(entry.getTermRegexPattern()).isEqualTo("manipulate NEAR{5} (?:price|spread|stock)");

            ResolvedPatternTree.Chain chain = (ResolvedPatternTree.Chain) entry.getResolvedPatternTree();
            assertThat(chain.getLeaves()).hasSize(2);
            assertThat(chain.getOperators()).containsExactly("NEAR");
            assertThat(chain.getDistances()).containsExactly(5);
        }

        @Test
        @DisplayName("A single-leaf term with resolvedPatterns but no patternMapping still parses (optional field)")
        void singleLeafResolvedPatternsWithoutPatternMapping() {
            String json = """
                {"results": [
                  {"termId": "%s::2", "compilationStatus": "PASS",
                   "regexPattern": ["(?:insidertrading|tradinginsider)"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "(?:insidertrading|tradinginsider)",
                   "hyperscanExpressionId": 2}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(2);

            assertThat(entry).isNotNull();
            assertThat(entry.requiresPerAreaEvaluation()).isTrue();
            ResolvedPatternTree.Chain chain = (ResolvedPatternTree.Chain) entry.getResolvedPatternTree();
            assertThat(chain.getLeaves()).hasSize(1);
            assertThat(chain.getOperators()).isEmpty();
        }

        @Test
        @DisplayName("A FOLLOWEDBY 3-leaf chain term (sample term 6 shape) parses correctly")
        void followedByChainTermParsesCorrectly() {
            String json = """
                {"results": [
                  {"termId": "%s::6", "compilationStatus": "PASS",
                   "regexPattern": ["avoidnow", "frontrun", "danger"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "avoidnow FOLLOWEDBY{4} frontrun FOLLOWEDBY{4} danger",
                   "hyperscanExpressionId": 6, "patternMapping": "(9&10&11)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(6);

            ResolvedPatternTree.Chain chain = (ResolvedPatternTree.Chain) entry.getResolvedPatternTree();
            assertThat(chain.getLeaves()).hasSize(3);
            assertThat(chain.getOperators()).containsExactly("FOLLOWEDBY", "FOLLOWEDBY");
            assertThat(chain.getDistances()).containsExactly(4, 4);
        }

        @Test
        @DisplayName("Leaf-count mismatch between resolvedPatterns' shape and regexPattern throws")
        void leafCountMismatchThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["a", "b"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "a NEAR{5} b NEAR{5} c",
                   "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
        }

        @Test
        @DisplayName("patternMapping id-count mismatch against regexPattern's leaf count throws")
        void patternMappingCountMismatchThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["a", "b"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "a NEAR{5} b",
                   "hyperscanExpressionId": 1, "patternMapping": "(7&8&9)"}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("patternMapping");
        }

        @Test
        @DisplayName("An AND NOT resolvedPatterns term WITH required/excluded ids is indexed and gets a coarse pre-filter")
        void andNotResolvedPatternsWithIds() {
            String json = """
                {"results": [
                  {"termId": "%s::7", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf", "exclleaf"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
                   "requiredExpressionIds": [30], "excludedExpressionIds": [31]}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry viaRequired = meta.termByAnyExpressionId(30);
            TermEntry viaExcluded = meta.termByAnyExpressionId(31);

            assertThat(viaRequired).isNotNull();
            assertThat(viaExcluded).isNotNull();
            assertThat(viaRequired.getTermNumber()).isEqualTo(7);
            assertThat(viaRequired.requiresPerAreaEvaluation()).isTrue();
            assertThat(viaRequired.hasCoarseExpressionId()).isTrue();
            assertThat(viaRequired.getResolvedPatternTree()).isInstanceOf(ResolvedPatternTree.AndNot.class);
            assertThat(meta.mandatoryPerAreaTerms()).isEmpty();
        }

        @Test
        @DisplayName("An AND NOT resolvedPatterns term with NO ids at all is only reachable via mandatoryPerAreaTerms()")
        void andNotResolvedPatternsWithoutIds() {
            String json = """
                {"results": [
                  {"termId": "%s::8", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf", "exclleaf"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);

            assertThat(meta.termByAnyExpressionId(anythingUnused())).isNull();
            assertThat(meta.termCount()).isEqualTo(1);
            assertThat(meta.mandatoryPerAreaTerms()).hasSize(1);
            TermEntry entry = meta.mandatoryPerAreaTerms().get(0);
            assertThat(entry.getTermNumber()).isEqualTo(8);
            assertThat(entry.hasCoarseExpressionId()).isFalse();
            assertThat(entry.requiresPerAreaEvaluation()).isTrue();
        }

        private int anythingUnused() {
            return 999999;
        }

        @Test
        @DisplayName("An AND NOT-shaped resolvedPatterns term with a native hyperscanExpressionId also populated throws")
        void andNotShapeWithHyperscanExpressionIdThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::7", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf", "exclleaf"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
                   "hyperscanExpressionId": 5}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("hyperscanExpressionId");
        }

        @Test
        @DisplayName("An AND NOT-shaped resolvedPatterns term with a patternMapping matching its own ids is accepted")
        void andNotShapeWithMatchingPatternMappingIsAccepted() {
            // Confirmed against a real compile-results.json: patternMapping legitimately documents
            // the required/excluded id formula (e.g. "((11&12&13)&!14)") alongside plain
            // requiredExpressionIds/excludedExpressionIds for a real AND NOT term — it is not, by
            // itself, evidence of native COMBINATION (only hyperscanExpressionId is).
            String json = """
                {"results": [
                  {"termId": "%s::7", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf"], "exclusionRegex": ["exclleaf"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
                   "requiredExpressionIds": [30], "excludedExpressionIds": [31],
                   "patternMapping": "(30&!31)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(30);

            assertThat(entry).isNotNull();
            assertThat(entry.getResolvedPatternTree()).isInstanceOf(ResolvedPatternTree.AndNot.class);
        }

        @Test
        @DisplayName("An AND NOT-shaped resolvedPatterns term whose patternMapping references the wrong ids throws")
        void andNotShapeWithMismatchedPatternMappingThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::7", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf"], "exclusionRegex": ["exclleaf"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
                   "requiredExpressionIds": [30], "excludedExpressionIds": [31],
                   "patternMapping": "(30&!99)"}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("patternMapping");
        }

        @Test
        @DisplayName("An AND NOT resolvedPatterns term with required leaves in regexPattern and the excluded leaf "
                + "in exclusionRegex (the real Compile Service shape) parses")
        void andNotResolvedPatternsWithSeparateExclusionRegexField() {
            String json = """
                {"results": [
                  {"termId": "%s::10", "compilationStatus": "PASS",
                   "regexPattern": ["a", "b", "c"],
                   "exclusionRegex": ["d"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "a NEAR{2} b NEAR{2} c AND NOT (d)",
                   "requiredExpressionIds": [11, 12, 13], "excludedExpressionIds": [14],
                   "patternMapping": "((11&12&13)&!14)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(11);

            assertThat(entry).isNotNull();
            assertThat(entry.getTermNumber()).isEqualTo(10);
            assertThat(entry.getResolvedPatternTree()).isInstanceOf(ResolvedPatternTree.AndNot.class);
            ResolvedPatternTree.AndNot andNot = (ResolvedPatternTree.AndNot) entry.getResolvedPatternTree();
            assertThat(((ResolvedPatternTree.Chain) andNot.getRequired()).getLeaves()).hasSize(3);
            assertThat(((ResolvedPatternTree.Chain) andNot.getExcluded()).getLeaves()).hasSize(1);
        }

        @Test
        @DisplayName("requiresExclusionCheck=false with an AND NOT-shaped resolvedPatterns string throws")
        void requiresExclusionCheckDisagreesWithAndNotShapeThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::7", "compilationStatus": "PASS",
                   "regexPattern": ["reqleaf", "exclleaf"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "reqleaf AND NOT (exclleaf)",
                   "requiredExpressionIds": [30], "excludedExpressionIds": [31]}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("requiresExclusionCheck");
        }

        @Test
        @DisplayName("requiresExclusionCheck=true with a plain-chain resolvedPatterns string (no AND NOT) throws")
        void requiresExclusionCheckDisagreesWithChainShapeThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "price"],
                   "requiresExclusionCheck": true,
                   "resolvedPatterns": "manipulate NEAR{5} price",
                   "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("requiresExclusionCheck");
        }

        @Test
        @DisplayName("A file mixing a legacy (translatedPattern) term and a new (resolvedPatterns) term parses both")
        void mixedOldAndNewSchemaTermsCoexist() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["legacy"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 1},
                  {"termId": "%s::2", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "price"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "manipulate NEAR{5} price",
                   "hyperscanExpressionId": 2, "patternMapping": "(7&8)"}
                ]}
                """.formatted(FEATURE, FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);

            assertThat(meta.termCount()).isEqualTo(2);
            TermEntry legacy = meta.termByAnyExpressionId(1);
            TermEntry modern = meta.termByAnyExpressionId(2);
            assertThat(legacy.requiresPerAreaEvaluation()).isFalse();
            assertThat(legacy.isNativelyResolved()).isTrue();
            assertThat(modern.requiresPerAreaEvaluation()).isTrue();
        }
    }

    @Nested
    @DisplayName("Malformed / inconsistent input is rejected clearly, not silently mishandled")
    class ErrorHandling {

        @Test
        @DisplayName("Null JSON throws TermMetadataParseException")
        void nullJsonThrows() {
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, null))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
        }

        @Test
        @DisplayName("Blank JSON throws TermMetadataParseException")
        void blankJsonThrows() {
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, "   "))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
        }

        @Test
        @DisplayName("Malformed JSON syntax throws TermMetadataParseException, not an unchecked parser exception")
        void malformedJsonThrows() {
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, "{not valid json"))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
        }

        @Test
        @DisplayName("A termId without a parseable '::<n>' suffix throws")
        void missingTermNumberThrows() {
            String json = """
                {"results": [
                  {"termId": "no-separator-here", "compilationStatus": "PASS",
                   "translatedPattern": ["x"], "requiresExclusionCheck": false, "hyperscanExpressionId": 1}
                ]}
                """;
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("::<n>");
        }

        @Test
        @DisplayName("A PASS term with neither hyperscanExpressionId nor requiredExpressionIds throws")
        void missingAllIdsThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["x"],
                   "requiresExclusionCheck": false}
                ]}
                """.formatted(FEATURE);
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
        }

        @Test
        @DisplayName("Two different terms claiming the SAME expression id throws — malformed/stale JSON, " +
                     "never silently resolved to the wrong one")
        void duplicateExpressionIdThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["a"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 5},
                  {"termId": "%s::2", "compilationStatus": "PASS", "translatedPattern": ["b"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 5}
                ]}
                """.formatted(FEATURE, FEATURE);
            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("claimed by both");
        }

        @Test
        @DisplayName("Empty results array parses successfully to an empty, valid metadata object")
        void emptyResultsIsValid() {
            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, "{\"results\": []}");
            assertThat(meta.termCount()).isEqualTo(0);
            assertThat(meta.getFeature()).isEqualTo(FEATURE);
        }
    }
}
