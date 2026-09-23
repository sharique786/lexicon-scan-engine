package com.db.macs3.ecomms.spectre.model.termmeta;

import com.db.macs3.ecomms.spectre.model.termmeta.TermExpressionMetadata.TermEntry;
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
        @DisplayName("termDescription is carried through verbatim — the original analyst-authored lexicon "
                + "term text, for lexicon-hit-summary.term_dtls.term_description")
        void termDescriptionIsCarriedThroughVerbatim() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "termDescription": "(manipulate) NEAR{5} ((price) OR (spread) OR (stock))",
                   "compilationStatus": "PASS", "translatedPattern": ["insider"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(1);

            assertThat(entry.getTermDescription()).isEqualTo("(manipulate) NEAR{5} ((price) OR (spread) OR (stock))");
        }

        @Test
        @DisplayName("A term with no termDescription field at all parses fine, with a null description")
        void missingTermDescriptionIsNullNotAnError() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["insider"],
                   "requiresExclusionCheck": false, "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(1);

            assertThat(entry.getTermDescription()).isNull();
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
        @DisplayName("A nested proximity group (sample term 26 shape) parses into a Chain whose second "
                + "element is itself a nested Chain, not a third flat leaf")
        void nestedChainElementParsesCorrectly() {
            // Real shape observed from the Compile Service: the right-hand side of the outer NEAR{5}
            // is itself parenthesized as its own 2-leaf NEAR{5} chain, not flattened into a 3-leaf
            // sequential chain the way the Compile Service used to emit this term.
            String json = """
                {"results": [
                  {"termId": "%s::26", "compilationStatus": "PASS",
                   "regexPattern": ["(?:manipulate|front run)", "(?:price|spread)", "stock"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "(?:manipulate|front run) NEAR{5} ((?:price|spread) NEAR{5} stock)",
                   "hyperscanExpressionId": 26, "patternMapping": "(56&(57&58))"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(26);

            assertThat(entry).isNotNull();
            assertThat(entry.requiresPerAreaEvaluation()).isTrue();

            ResolvedPatternTree.Chain outer = (ResolvedPatternTree.Chain) entry.getResolvedPatternTree();
            assertThat(outer.getLeaves()).hasSize(2);
            assertThat(outer.getOperators()).containsExactly("NEAR");
            assertThat(outer.getDistances()).containsExactly(5);
            assertThat(outer.getLeaves().get(0)).isInstanceOf(ResolvedPatternTree.Leaf.class);

            ResolvedPatternTree.Chain nested = (ResolvedPatternTree.Chain) outer.getLeaves().get(1);
            assertThat(nested.getLeaves()).hasSize(2);
            assertThat(nested.getOperators()).containsExactly("NEAR");
            assertThat(nested.getDistances()).containsExactly(5);

            // Total leaf count across the whole (nested) tree must still agree with patternMapping's
            // id count — this is what previously threw "regexPattern has more leaves than
            // resolvedPatterns' shape implies" before nested-Chain support existed.
            assertThat(ResolvedPatternTree.countLeaves(outer)).isEqualTo(3);
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
            TermEntry entry = meta.mandatoryPerAreaTerms().getFirst();
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
    @DisplayName("Inline-compiled proximity — one regexPattern entry, resolvedPatterns still shows NEAR/FOLLOWEDBY")
    class InlineCompiledProximityTerms {

        /**
         * REGRESSION — the Compile Service began compiling NEAR/FOLLOWEDBY terms that fit in Hyperscan as
         * ONE regex with the proximity baked in, while leaving resolvedPatterns in operator form. Zipping
         * that 2-leaf shape against 1 regex threw "resolvedPatterns' shape implies more leaves than
         * regexPattern provides", failing the whole feature's load (every message → failure row).
         */
        @Test
        @DisplayName("REGRESSION lexicon_research_1::9 shape: parses without throwing, no tree, resolved by id")
        void inlineProximityTermParsesWithoutTree() {
            String json = """
                {"results": [
                  {"termId": "%s::9", "termDescription": "keep FOLLOWEDBY{3} mouth shut", "compilationStatus": "PASS",
                   "regexPattern": ["\\\\bkeep\\\\b(?:\\\\s+\\\\S+){0,3}\\\\s+\\\\bmouth shut\\\\b"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "\\\\bkeep\\\\b FOLLOWEDBY{3} \\\\bmouth shut\\\\b",
                   "hyperscanExpressionId": 9},
                  {"termId": "%s::22", "compilationStatus": "PASS", "regexPattern": ["\\\\blaunder\\\\b"],
                   "requiresExclusionCheck": false, "resolvedPatterns": "\\\\blaunder\\\\b",
                   "hyperscanExpressionId": 22}
                ]}
                """.formatted(FEATURE, FEATURE);

            TermExpressionMetadata metadata = TermExpressionMetadata.parse(FEATURE, json);

            TermEntry inline = metadata.termByAnyExpressionId(9);
            assertThat(inline.getTermNumber()).isEqualTo(9);
            assertThat(inline.getResolvedPatternTree()).isNull();
            assertThat(inline.requiresPerAreaEvaluation()).isFalse();
            assertThat(inline.getRequiredExpressionIds()).containsExactly(9);
            assertThat(inline.getTermRegexPattern()).isEqualTo("\\bkeep\\b FOLLOWEDBY{3} \\bmouth shut\\b");
            // A plain single-leaf term (resolvedPatterns == the regex) is untouched by this path.
            assertThat(metadata.termByAnyExpressionId(22).getResolvedPatternTree()).isNotNull();
        }
    }

    @Nested
    @DisplayName("Plain AND terms — a NEAR/FOLLOWEDBY chain plainly ANDed with a further leaf, not AND NOT")
    class PlainAndTerms {

        /**
         * REGRESSION — reproduces the real production failure: {@code HyperscanBundleLoader.buildBundle}
         * threw {@code HyperscanFileLoadException} wrapping "regexPattern has more leaves than
         * resolvedPatterns' shape implies (3 provided)" for {@code lexicon_research_3::2}, whose
         * {@code resolvedPatterns} is a 2-leaf {@code NEAR{5}} chain plainly ANDed (not AND NOT) with a
         * third leaf — {@link ResolvedPatternTree#parseShape} only recognised
         * {@code NEAR}/{@code FOLLOWEDBY}/{@code AND NOT}, so the plain {@code " AND ("} text was silently
         * absorbed into the chain's final segment instead of ending it, undercounting the shape's leaves
         * by one against the 3 real {@code regexPattern} entries.
         */
        @Test
        @DisplayName("REGRESSION lexicon_research_3::2 shape: 2-leaf NEAR chain AND a third leaf parses "
                     + "without throwing, into an And(Chain[2], Chain[1])")
        void nearChainPlainlyAndedWithThirdLeaf_parsesCorrectly() {
            String json = """
                {"results": [
                  {"termId": "%s::2", "compilationStatus": "PASS",
                   "regexPattern": [
                     "(?:any color|any steer|any read|any preview|any hint|heads up|early look|what are you hearing|what's the view|where are you coming out|any whispers)",
                     "(?:analyst|research|Information|report|note|rating|view|call|model|target|conclusion|publication)",
                     "(?:going to publish|about to publish|before it goes out|pre-publication|ahead of the print|upcoming)"
                   ],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "(?:any color|any steer|any read|any preview|any hint|heads up|early look|what are you hearing|what's the view|where are you coming out|any whispers) NEAR{5} (?:analyst|research|Information|report|note|rating|view|call|model|target|conclusion|publication) AND (?:going to publish|about to publish|before it goes out|pre-publication|ahead of the print|upcoming)",
                   "hyperscanExpressionId": 2, "patternMapping": "(20&21&22)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(2);

            assertThat(entry).isNotNull();
            assertThat(entry.getTermNumber()).isEqualTo(2);
            assertThat(entry.isRequiresExclusionCheck()).isFalse();
            assertThat(entry.requiresPerAreaEvaluation()).isTrue();
            assertThat(entry.hasCoarseExpressionId()).isTrue();
            assertThat(entry.getRequiredExpressionIds()).containsExactly(2);

            ResolvedPatternTree.And and = (ResolvedPatternTree.And) entry.getResolvedPatternTree();
            ResolvedPatternTree.Chain nearChain = (ResolvedPatternTree.Chain) and.getLeft();
            ResolvedPatternTree.Chain thirdLeafChain = (ResolvedPatternTree.Chain) and.getRight();
            assertThat(nearChain.getLeaves()).hasSize(2);
            assertThat(nearChain.getOperators()).containsExactly("NEAR");
            assertThat(nearChain.getDistances()).containsExactly(5);
            assertThat(thirdLeafChain.getLeaves()).hasSize(1);
        }

        @Test
        @DisplayName("A simpler 2-leaf-chain AND 1-leaf shape also parses, into And(Chain[2], Chain[1])")
        void simpleChainAndSingleLeaf_parsesCorrectly() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "(?:price|spread)", "(?:disclosure|filing)"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "manipulate NEAR{5} (?:price|spread) AND (?:disclosure|filing)",
                   "hyperscanExpressionId": 1, "patternMapping": "(7&8&9)"}
                ]}
                """.formatted(FEATURE);

            TermExpressionMetadata meta = TermExpressionMetadata.parse(FEATURE, json);
            TermEntry entry = meta.termByAnyExpressionId(1);

            ResolvedPatternTree.And and = (ResolvedPatternTree.And) entry.getResolvedPatternTree();
            assertThat(((ResolvedPatternTree.Chain) and.getLeft()).getLeaves()).hasSize(2);
            assertThat(((ResolvedPatternTree.Chain) and.getRight()).getLeaves()).hasSize(1);
        }

        @Test
        @DisplayName("patternMapping id-count is validated against the And tree's TOTAL leaf count, "
                     + "across both sides, not just one chain")
        void patternMappingCountValidatedAcrossBothSidesOfAnd() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "(?:price|spread)", "(?:disclosure|filing)"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "manipulate NEAR{5} (?:price|spread) AND (?:disclosure|filing)",
                   "hyperscanExpressionId": 1, "patternMapping": "(7&8)"}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class)
                    .hasMessageContaining("patternMapping");
        }

        @Test
        @DisplayName("Leaf-count mismatch between an And shape and regexPattern still throws, same as a plain chain")
        void leafCountMismatchOnAndShapeThrows() {
            String json = """
                {"results": [
                  {"termId": "%s::1", "compilationStatus": "PASS",
                   "regexPattern": ["manipulate", "(?:price|spread)"],
                   "requiresExclusionCheck": false,
                   "resolvedPatterns": "manipulate NEAR{5} (?:price|spread) AND (?:disclosure|filing)",
                   "hyperscanExpressionId": 1}
                ]}
                """.formatted(FEATURE);

            assertThatThrownBy(() -> TermExpressionMetadata.parse(FEATURE, json))
                    .isInstanceOf(TermExpressionMetadata.TermMetadataParseException.class);
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
