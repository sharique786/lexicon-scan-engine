package com.db.macs3.ecomms.spectre.scanengine.model.feature;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Parsed shape of {@code FeatureDecisionRow.featureDefinitionJson} — the
 * {@code feature_definition} JSON column from
 * {@code vw_src_msg_lexicon_decision_mapping}.
 *
 * <pre>
 * {
 *   "featureName": "lexicon_market_cond_1",
 *   "featureType": "Lexicon",
 *   "isNoiseReduction": false,
 *   "body": {
 *     "feature": "lexicon_market_cond-1",
 *     "totalTermsCount": 10,
 *     "minimumHits": 3,
 *     "scope": ["Message Body", "Attachment"]
 *   }
 * }
 * </pre>
 *
 * <p>{@code body.feature} is the value that resolves BOTH the {@code .hdb}
 * filename to load AND the prefix of every {@code term_id} this feature
 * produces ({@code <body.feature>::<index>}) — verbatim, hyphens and all;
 * it is never re-derived from {@code featureName} or the view's own
 * {@code feature_name}/{@code features_to_apply} columns, which are
 * independent, human-readable labels only.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 * {@code @JsonCreator}/{@code @JsonProperty} on the constructor is the
 * standard, portable way to have Jackson deserialise into an immutable
 * class without records or JavaBean setters.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class FeatureDefinition implements Serializable {

    private final String featureName;
    private final String featureType;
    private final boolean isNoiseReduction;
    private final Body body;

    @JsonCreator
    public FeatureDefinition(
            @JsonProperty("featureName") String featureName,
            @JsonProperty("featureType") String featureType,
            @JsonProperty("isNoiseReduction") boolean isNoiseReduction,
            @JsonProperty("body") Body body) {
        this.featureName = featureName;
        this.featureType = featureType;
        this.isNoiseReduction = isNoiseReduction;
        this.body = body;
    }

    public String featureName() { return featureName; }
    public String featureType() { return featureType; }
    public boolean isNoiseReduction() { return isNoiseReduction; }
    public Body body() { return body; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeatureDefinition)) return false;
        FeatureDefinition other = (FeatureDefinition) o;
        return isNoiseReduction == other.isNoiseReduction
                && Objects.equals(featureName, other.featureName)
                && Objects.equals(featureType, other.featureType)
                && Objects.equals(body, other.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, featureType, isNoiseReduction, body);
    }

    @Override
    public String toString() {
        return "FeatureDefinition[featureName=" + featureName + ", featureType=" + featureType
                + ", isNoiseReduction=" + isNoiseReduction + ", body=" + body + "]";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Body implements Serializable {

        private final String feature;
        private final Integer totalTermsCount;
        private final Integer minimumHits;
        private final List<String> scope;

        @JsonCreator
        public Body(
                @JsonProperty("feature") String feature,
                @JsonProperty("totalTermsCount") Integer totalTermsCount,
                @JsonProperty("minimumHits") Integer minimumHits,
                @JsonProperty("scope") List<String> scope) {
            this.feature = feature;
            this.totalTermsCount = totalTermsCount;
            this.minimumHits = minimumHits;
            this.scope = scope;
        }

        public String feature() { return feature; }
        public Integer totalTermsCount() { return totalTermsCount; }
        public Integer minimumHits() { return minimumHits; }
        public List<String> scope() { return scope; }

        /**
         * Case-insensitive scope membership check — sample data mixes
         * casing ({@code "subject"} vs {@code "Message Body"}), so scope
         * matching never relies on exact case.
         */
        public boolean hasScope(String candidate) {
            if (scope == null || candidate == null) {
                return false;
            }
            return scope.stream().anyMatch(s -> s != null && s.equalsIgnoreCase(candidate));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Body)) return false;
            Body other = (Body) o;
            return Objects.equals(feature, other.feature)
                    && Objects.equals(totalTermsCount, other.totalTermsCount)
                    && Objects.equals(minimumHits, other.minimumHits)
                    && Objects.equals(scope, other.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feature, totalTermsCount, minimumHits, scope);
        }

        @Override
        public String toString() {
            return "Body[feature=" + feature + ", totalTermsCount=" + totalTermsCount
                    + ", minimumHits=" + minimumHits + ", scope=" + scope + "]";
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses a raw {@code feature_definition} JSON string.
     *
     * @throws FeatureDefinitionParseException if the JSON is malformed or
     *                                           missing the required {@code body.feature} value —
     *                                           every feature this engine scans with MUST resolve to
     *                                           a {@code .hdb} filename, so a definition without one
     *                                           is treated as an error, not silently skipped
     */
    public static FeatureDefinition parse(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new FeatureDefinitionParseException("feature_definition JSON is null or blank");
        }
        FeatureDefinition parsed;
        try {
            parsed = MAPPER.readValue(rawJson, FeatureDefinition.class);
        } catch (IOException e) {
            throw new FeatureDefinitionParseException(
                    "Could not parse feature_definition JSON: " + e.getMessage(), e);
        }
        if (parsed.body() == null || parsed.body().feature() == null || parsed.body().feature().isBlank()) {
            throw new FeatureDefinitionParseException(
                    "feature_definition JSON is missing required body.feature value: " + rawJson);
        }
        return parsed;
    }

    /** Thrown by {@link #parse} on malformed or incomplete {@code feature_definition} JSON. */
    public static final class FeatureDefinitionParseException extends RuntimeException {
        public FeatureDefinitionParseException(String message) {
            super(message);
        }
        public FeatureDefinitionParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
