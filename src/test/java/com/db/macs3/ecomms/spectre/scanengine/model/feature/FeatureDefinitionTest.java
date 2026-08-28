package com.db.macs3.ecomms.spectre.scanengine.model.feature;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link FeatureDefinition} parsing against the exact sample JSON
 * shapes from the requirements' "View Data" reference, including the
 * case-insensitive scope matching the sample data itself needs (mixed
 * casing: {@code "subject"} vs {@code "Message Body"}).
 */
@DisplayName("FeatureDefinition")
class FeatureDefinitionTest {

    private static final String SAMPLE_JSON =
            "{"
            + "  \"featureName\": \"lexicon_market_cond_1\","
            + "  \"featureType\": \"Lexicon\","
            + "  \"isNoiseReduction\": false,"
            + "  \"body\": {"
            + "    \"feature\": \"lexicon_market_cond-1\","
            + "    \"totalTermsCount\": 10,"
            + "    \"minimumHits\": 3,"
            + "    \"scope\": [\"Message Body\", \"Attachment\"]"
            + "  }"
            + "}";

    private static final String MIXED_CASE_SCOPE_JSON =
            "{"
            + "  \"featureName\": \"lexicon_market_cond_2\","
            + "  \"featureType\": \"Lexicon\","
            + "  \"isNoiseReduction\": true,"
            + "  \"body\": {"
            + "    \"feature\": \"lexicon_market_cond-2\","
            + "    \"totalTermsCount\": 20,"
            + "    \"minimumHits\": 5,"
            + "    \"scope\": [\"subject\", \"Message Body\"]"
            + "  }"
            + "}";

    @Nested
    @DisplayName("parsing the sample View Data JSON")
    class ParsingSampleJson {

        @Test
        @DisplayName("parses root-level fields correctly")
        void parsesRootFields() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.featureName()).isEqualTo("lexicon_market_cond_1");
            assertThat(fd.featureType()).isEqualTo("Lexicon");
            assertThat(fd.isNoiseReduction()).isFalse();
        }

        @Test
        @DisplayName("body.feature is parsed verbatim, hyphen and all — never re-derived from featureName")
        void parsesBodyFeatureVerbatim() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().feature()).isEqualTo("lexicon_market_cond-1");
        }

        @Test
        @DisplayName("parses totalTermsCount and minimumHits")
        void parsesCounts() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().totalTermsCount()).isEqualTo(10);
            assertThat(fd.body().minimumHits()).isEqualTo(3);
        }

        @Test
        @DisplayName("parses the scope array")
        void parsesScope() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().scope()).containsExactly("Message Body", "Attachment");
        }
    }

    @Nested
    @DisplayName("case-insensitive scope matching")
    class ScopeMatching {

        @Test
        @DisplayName("hasScope matches exact case")
        void matchesExactCase() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().hasScope("Message Body")).isTrue();
        }

        @Test
        @DisplayName("hasScope matches different case (lowercase 'attachment' vs stored 'Attachment')")
        void matchesDifferentCase() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().hasScope("attachment")).isTrue();
        }

        @Test
        @DisplayName("hasScope returns false for a scope value not present")
        void returnsFalseForAbsentScope() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.body().hasScope("subject")).isFalse();
        }

        @Test
        @DisplayName("handles the sample data's own mixed casing: lowercase 'subject', title-case 'Message Body'")
        void handlesSampleDataMixedCasing() {
            FeatureDefinition fd = FeatureDefinition.parse(MIXED_CASE_SCOPE_JSON);
            assertThat(fd.body().hasScope("subject")).isTrue();
            assertThat(fd.body().hasScope("SUBJECT")).isTrue();
            assertThat(fd.body().hasScope("Message Body")).isTrue();
        }
    }

    @Nested
    @DisplayName("error handling")
    class ErrorHandling {

        @Test
        @DisplayName("throws on missing body.feature — every scanned feature must resolve to an .hdb filename")
        void throwsOnMissingBodyFeature() {
            assertThatThrownBy(() -> FeatureDefinition.parse("{\"featureName\":\"x\",\"body\":{}}"))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class)
                    .hasMessageContaining("body.feature");
        }

        @Test
        @DisplayName("throws on malformed JSON")
        void throwsOnMalformedJson() {
            assertThatThrownBy(() -> FeatureDefinition.parse("not json at all {{{"))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class);
        }

        @Test
        @DisplayName("throws on null input")
        void throwsOnNull() {
            assertThatThrownBy(() -> FeatureDefinition.parse(null))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class);
        }

        @Test
        @DisplayName("throws on blank input")
        void throwsOnBlank() {
            assertThatThrownBy(() -> FeatureDefinition.parse("   "))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class);
        }
    }
}
