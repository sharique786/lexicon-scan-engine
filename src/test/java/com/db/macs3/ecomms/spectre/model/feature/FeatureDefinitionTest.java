package com.db.macs3.ecomms.spectre.model.feature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link FeatureDefinition} parsing against the exact sample JSON
 * shape from the requirements' "View Data" reference — {@code featureType}
 * lowercase ({@code "lexicon"}), {@code body.objectId} as a JSON string,
 * root-level {@code isNoiseReduction} as a {@code "Y"}/{@code "N"} string —
 * plus the case-insensitive scope matching the sample data itself needs
 * (mixed casing: {@code "subject"} vs {@code "Message Body"}).
 */
@DisplayName("FeatureDefinition")
class FeatureDefinitionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SAMPLE_BODY =
            "{"
            + "    \"id\": 1,"
            + "    \"lexiconName\": \"lexicon_market_cond-1\","
            + "    \"objectId\": \"2\","
            + "    \"totalTermsCount\": 10,"
            + "    \"minimumHits\": 3,"
            + "    \"scope\": [\"Message Body\", \"Attachment\"]"
            + "  }";

    private static final String MIXED_CASE_SCOPE_BODY =
            "{"
            + "    \"id\": 2,"
            + "    \"lexiconName\": \"lexicon_market_cond-2\","
            + "    \"objectId\": \"3\","
            + "    \"totalTermsCount\": 20,"
            + "    \"minimumHits\": 5,"
            + "    \"scope\": [\"subject\", \"Message Body\"]"
            + "  }";

    /** The current view shape: {@code body} is a JSON-encoded STRING holding the body object. */
    private static String withStringBody(String bodyJson, String featureId, String featureName, String isNoiseReduction) {
        try {
            return "{\"body\": " + MAPPER.writeValueAsString(bodyJson)
                    + ", \"featureId\": \"" + featureId + "\", \"featureName\": \"" + featureName + "\","
                    + " \"featureType\": \"lexicon\", \"isNoiseReduction\": \"" + isNoiseReduction + "\"}";
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The previous view shape: {@code body} is a nested JSON object. */
    private static String withObjectBody(String bodyJson, String featureId, String featureName, String isNoiseReduction) {
        return "{\"body\": " + bodyJson
                + ", \"featureId\": \"" + featureId + "\", \"featureName\": \"" + featureName + "\","
                + " \"featureType\": \"lexicon\", \"isNoiseReduction\": \"" + isNoiseReduction + "\"}";
    }

    private static final String SAMPLE_JSON = withStringBody(SAMPLE_BODY, "1", "lexicon_market_cond_1", "N");

    private static final String MIXED_CASE_SCOPE_JSON =
            withStringBody(MIXED_CASE_SCOPE_BODY, "2", "lexicon_market_cond_2", "Y");

    @Nested
    @DisplayName("parsing the sample View Data JSON")
    class ParsingSampleJson {

        @Test
        @DisplayName("parses root-level fields correctly")
        void parsesRootFields() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getFeatureId()).isEqualTo("1");
            assertThat(fd.getFeatureName()).isEqualTo("lexicon_market_cond_1");
            assertThat(fd.getFeatureType()).isEqualTo("lexicon");
            assertThat(fd.getIsNoiseReduction()).isEqualTo("N");
            assertThat(fd.isNoiseReductionFlagSet()).isFalse();
        }

        @Test
        @DisplayName("body.lexiconName is parsed verbatim, hyphen and all — never re-derived from featureName")
        void parsesBodyLexiconNameVerbatim() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().getLexiconName()).isEqualTo("lexicon_market_cond-1");
        }

        @Test
        @DisplayName("parses body.id (still a number) and body.objectId (now a string)")
        void parsesBodyIds() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().getId()).isEqualTo(1);
            assertThat(fd.getBody().getObjectId()).isEqualTo("2");
        }

        @Test
        @DisplayName("parses totalTermsCount and minimumHits")
        void parsesCounts() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().getTotalTermsCount()).isEqualTo(10);
            assertThat(fd.getBody().getMinimumHits()).isEqualTo(3);
        }

        @Test
        @DisplayName("parses the scope array")
        void parsesScope() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().getScope()).containsExactly("Message Body", "Attachment");
        }
    }

    @Nested
    @DisplayName("body delivered as a JSON-encoded string")
    class StringBody {

        @Test
        @DisplayName("the literal string-body shape from the view parses into FeatureDefinition.Body")
        void parsesLiteralStringBody() {
            String json = "{\"body\": \"{\\\"id\\\": 1, \\\"lexiconName\\\": \\\"lexicon_market_cond-1\\\", "
                    + "\\\"objectId\\\": \\\"2\\\", \\\"totalTermsCount\\\": 10, \\\"minimumHits\\\": 3, "
                    + "\\\"scope\\\": [\\\"Message Body\\\", \\\"Attachment\\\"]}\", "
                    + "\"featureId\": \"1\", \"featureName\": \"lexicon_market_cond_1\", "
                    + "\"featureType\": \"lexicon\", \"isNoiseReduction\": \"N\"}";

            FeatureDefinition fd = FeatureDefinition.parse(json);

            assertThat(fd.getFeatureId()).isEqualTo("1");
            assertThat(fd.getFeatureName()).isEqualTo("lexicon_market_cond_1");
            assertThat(fd.getFeatureType()).isEqualTo("lexicon");
            assertThat(fd.isNoiseReductionFlagSet()).isFalse();
            assertThat(fd.getBody().getId()).isEqualTo(1);
            assertThat(fd.getBody().getLexiconName()).isEqualTo("lexicon_market_cond-1");
            assertThat(fd.getBody().getObjectId()).isEqualTo("2");
            assertThat(fd.getBody().getTotalTermsCount()).isEqualTo(10);
            assertThat(fd.getBody().getMinimumHits()).isEqualTo(3);
            assertThat(fd.getBody().getScope()).containsExactly("Message Body", "Attachment");
        }

        @Test
        @DisplayName("string body and the previous nested-object body parse to equal FeatureDefinitions")
        void stringAndObjectBodiesAreEquivalent() {
            FeatureDefinition fromString = FeatureDefinition.parse(SAMPLE_JSON);
            FeatureDefinition fromObject =
                    FeatureDefinition.parse(withObjectBody(SAMPLE_BODY, "1", "lexicon_market_cond_1", "N"));

            assertThat(fromString).isEqualTo(fromObject);
        }

        @Test
        @DisplayName("unknown fields inside the string body are ignored")
        void ignoresUnknownBodyFields() {
            String body = SAMPLE_BODY.replace("\"id\": 1,", "\"id\": 1, \"somethingNew\": true,");
            FeatureDefinition fd = FeatureDefinition.parse(withStringBody(body, "1", "x", "N"));
            assertThat(fd.getBody().getLexiconName()).isEqualTo("lexicon_market_cond-1");
        }

        @Test
        @DisplayName("a string body without lexiconName throws the missing body.lexiconName error")
        void stringBodyWithoutLexiconNameThrows() {
            assertThatThrownBy(() -> FeatureDefinition.parse(withStringBody("{\"id\": 1}", "1", "x", "N")))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class)
                    .hasMessageContaining("body.lexiconName");
        }

        @Test
        @DisplayName("a string body that is not valid JSON throws FeatureDefinitionParseException")
        void malformedStringBodyThrows() {
            assertThatThrownBy(() -> FeatureDefinition.parse(withStringBody("not json {{{", "1", "x", "N")))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class);
        }

        @Test
        @DisplayName("a blank string body throws the missing body.lexiconName error")
        void blankStringBodyThrows() {
            assertThatThrownBy(() -> FeatureDefinition.parse(withStringBody("", "1", "x", "N")))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class);
        }
    }

    @Nested
    @DisplayName("isNoiseReduction: \"Y\"/\"N\" string, case-insensitive")
    class IsNoiseReductionFlag {

        @Test
        @DisplayName("\"N\" resolves to false")
        void nResolvesToFalse() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.isNoiseReductionFlagSet()).isFalse();
        }

        @Test
        @DisplayName("\"Y\" resolves to true")
        void yResolvesToTrue() {
            FeatureDefinition fd = FeatureDefinition.parse(MIXED_CASE_SCOPE_JSON);
            assertThat(fd.isNoiseReductionFlagSet()).isTrue();
        }

        @Test
        @DisplayName("lowercase \"y\" also resolves to true")
        void lowercaseYResolvesToTrue() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON.replace("\"N\"", "\"y\""));
            assertThat(fd.getIsNoiseReduction()).isEqualTo("y");
            assertThat(fd.isNoiseReductionFlagSet()).isTrue();
        }

        @Test
        @DisplayName("lowercase \"n\" resolves to false")
        void lowercaseNResolvesToFalse() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON.replace("\"N\"", "\"n\""));
            assertThat(fd.isNoiseReductionFlagSet()).isFalse();
        }
    }

    @Nested
    @DisplayName("case-insensitive scope matching")
    class ScopeMatching {

        @Test
        @DisplayName("hasScope matches exact case")
        void matchesExactCase() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().hasScope("Message Body")).isTrue();
        }

        @Test
        @DisplayName("hasScope matches different case (lowercase 'attachment' vs stored 'Attachment')")
        void matchesDifferentCase() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().hasScope("attachment")).isTrue();
        }

        @Test
        @DisplayName("hasScope matches fully uppercase candidate against a title-case stored value")
        void matchesUppercaseCandidate() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().hasScope("MESSAGE BODY")).isTrue();
            assertThat(fd.getBody().hasScope("ATTACHMENT")).isTrue();
        }

        @Test
        @DisplayName("hasScope matches a randomly mixed-case candidate")
        void matchesMixedCaseCandidate() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().hasScope("mEsSaGe BoDy")).isTrue();
        }

        @Test
        @DisplayName("hasScope returns false for a scope value not present")
        void returnsFalseForAbsentScope() {
            FeatureDefinition fd = FeatureDefinition.parse(SAMPLE_JSON);
            assertThat(fd.getBody().hasScope("subject")).isFalse();
            assertThat(fd.getBody().hasScope("SUBJECT")).isFalse();
        }

        @Test
        @DisplayName("handles the sample data's own mixed casing: lowercase 'subject', title-case 'Message Body'")
        void handlesSampleDataMixedCasing() {
            FeatureDefinition fd = FeatureDefinition.parse(MIXED_CASE_SCOPE_JSON);
            assertThat(fd.getBody().hasScope("subject")).isTrue();
            assertThat(fd.getBody().hasScope("SUBJECT")).isTrue();
            assertThat(fd.getBody().hasScope("Message Body")).isTrue();
            assertThat(fd.getBody().hasScope("message body")).isTrue();
        }

        @Test
        @DisplayName("hasScope returns false when the scope array itself is empty")
        void returnsFalseForEmptyScope() {
            String json = withStringBody(SAMPLE_BODY.replace("[\"Message Body\", \"Attachment\"]", "[]"),
                    "1", "lexicon_market_cond_1", "N");
            FeatureDefinition fd = FeatureDefinition.parse(json);
            assertThat(fd.getBody().hasScope("Message Body")).isFalse();
        }
    }

    @Nested
    @DisplayName("error handling")
    class ErrorHandling {

        @Test
        @DisplayName("throws on missing body.lexiconName — every scanned feature must resolve to an .hdb filename")
        void throwsOnMissingBodyLexiconName() {
            assertThatThrownBy(() -> FeatureDefinition.parse("{\"featureName\":\"x\",\"body\":{}}"))
                    .isInstanceOf(FeatureDefinition.FeatureDefinitionParseException.class)
                    .hasMessageContaining("body.lexiconName");
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
