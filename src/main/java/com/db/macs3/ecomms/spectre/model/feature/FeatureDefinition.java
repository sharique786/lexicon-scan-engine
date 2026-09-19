package com.db.macs3.ecomms.spectre.model.feature;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Parsed shape of {@code FeatureDecisionRow.getFeatureDefinitionJson} — the
 * {@code feature_definition} JSON column from
 * {@code vw_src_msg_lexicon_decision_mapping}.
 *
 * <pre>
 * {
 *   "body": {
 *     "id": 1,
 *     "lexiconName": "lexicon_market_cond_4",
 *     "objectId": "2",
 *     "scope": ["Message Body", "Attachment", "Subject"],
 *     "totalTermsCount": 17,
 *     "minimumHits": 2
 *   },
 *   "featureId": "2",
 *   "featureName": "lexicon_market_cond_4",
 *   "featureType": "lexicon",
 *   "isNoiseReduction": "N"
 * }
 * </pre>
 *
 * <p>{@code body.lexiconName} (renamed from {@code body.feature} — a schema
 * change, not a rename this project chose) is the value that resolves BOTH
 * the {@code .hdb} filename to load AND the prefix of every {@code term_id}
 * this feature produces ({@code <body.lexiconName>::<index>}) — verbatim,
 * hyphens and all; it is never re-derived from {@code featureName}/
 * {@code featureId} or the view's own {@code feature_name}/
 * {@code features_to_apply} columns, which are independent, human-readable
 * labels only. {@code featureId} (root-level) and {@code body.id}/
 * {@code body.objectId} are carried through for completeness but are not
 * (yet) consumed by any scanning/output logic in this engine.
 *
 * <h2>Schema change: {@code featureType} casing, {@code body.objectId} and {@code isNoiseReduction} types</h2>
 * <p>Three further, independent value/type changes on top of the ones above:
 * {@code featureType} now arrives lowercase ({@code "lexicon"}, was
 * {@code "Lexicon"} — a plain value change, the field stays {@code String});
 * {@code body.objectId} now arrives as a JSON string (was a JSON number —
 * this class's {@link Body#getObjectId()} is therefore {@code String}, not
 * {@code Integer}); and root-level {@code isNoiseReduction} now arrives as
 * the {@code "Y"}/{@code "N"} string convention used elsewhere in this
 * platform (was a JSON boolean — see {@link #getIsNoiseReduction()}).
 * {@code body.scope}'s values ({@code "Message Body"}/{@code "Attachment"}/
 * {@code "Subject"}) are unchanged in shape (still a {@code List<String>})
 * but are — and, per {@link Body#hasScope}, always have been — matched
 * case-insensitively against a scanned area, since upstream data is known to
 * mix casing (e.g. {@code "subject"} vs {@code "Message Body"}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureDefinition implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String featureId;
    private String featureName;
    private String featureType;
    private String isNoiseReduction;
    private Body body;

    @JsonCreator
    public FeatureDefinition(
            @JsonProperty("featureId") String featureId,
            @JsonProperty("featureName") String featureName,
            @JsonProperty("featureType") String featureType,
            @JsonProperty("isNoiseReduction") String isNoiseReduction,
            @JsonProperty("body") Body body) {
        this.featureId = featureId;
        this.featureName = featureName;
        this.featureType = featureType;
        this.isNoiseReduction = isNoiseReduction;
        this.body = body;
    }

    public String getFeatureId() {
        return featureId;
    }

    public void setFeatureId(String featureId) {
        this.featureId = featureId;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public String getFeatureType() {
        return featureType;
    }

    public void setFeatureType(String featureType) {
        this.featureType = featureType;
    }

    /**
     * @return the raw {@code "Y"}/{@code "N"} value as delivered — see
     *         {@link #isNoiseReductionFlagSet()} for the boolean-flag reading of it
     */
    public String getIsNoiseReduction() {
        return isNoiseReduction;
    }

    public void setIsNoiseReduction(String isNoiseReduction) {
        this.isNoiseReduction = isNoiseReduction;
    }

    /**
     * @return true iff {@link #getIsNoiseReduction()} is {@code "Y"}, matched
     *         case-insensitively; false for {@code "N"}, null, blank, or any
     *         other value
     */
    public boolean isNoiseReductionFlagSet() {
        return "Y".equalsIgnoreCase(isNoiseReduction);
    }

    public Body getBody() {
        return body;
    }

    public void setBody(Body body) {
        this.body = body;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        FeatureDefinition other = (FeatureDefinition) obj;
        return Objects.equals(isNoiseReduction, other.isNoiseReduction)
                && Objects.equals(featureId, other.featureId)
                && Objects.equals(featureName, other.featureName)
                && Objects.equals(featureType, other.featureType)
                && Objects.equals(body, other.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureId, featureName, featureType, isNoiseReduction, body);
    }

    @Override
    public String toString() {
        return "FeatureDefinition[featureId=" + featureId + ", featureName=" + featureName
                + ", featureType=" + featureType + ", isNoiseReduction=" + isNoiseReduction
                + ", body=" + body + "]";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Body implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private Integer id;
        private String lexiconName;
        private String objectId;
        private Integer totalTermsCount;
        private Integer minimumHits;
        private List<String> scope;

        @JsonCreator
        public Body(
                @JsonProperty("id") Integer id,
                @JsonProperty("lexiconName") String lexiconName,
                @JsonProperty("objectId") String objectId,
                @JsonProperty("totalTermsCount") Integer totalTermsCount,
                @JsonProperty("minimumHits") Integer minimumHits,
                @JsonProperty("scope") List<String> scope) {
            this.id = id;
            this.lexiconName = lexiconName;
            this.objectId = objectId;
            this.totalTermsCount = totalTermsCount;
            this.minimumHits = minimumHits;
            this.scope = scope;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getLexiconName() {
            return lexiconName;
        }

        public void setLexiconName(String lexiconName) {
            this.lexiconName = lexiconName;
        }

        public String getObjectId() {
            return objectId;
        }

        public void setObjectId(String objectId) {
            this.objectId = objectId;
        }

        public Integer getTotalTermsCount() {
            return totalTermsCount;
        }

        public void setTotalTermsCount(Integer totalTermsCount) {
            this.totalTermsCount = totalTermsCount;
        }

        public Integer getMinimumHits() {
            return minimumHits;
        }

        public void setMinimumHits(Integer minimumHits) {
            this.minimumHits = minimumHits;
        }

        public List<String> getScope() {
            return scope;
        }

        public void setScope(List<String> scope) {
            this.scope = scope;
        }

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
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            Body other = (Body) obj;
            return Objects.equals(id, other.id)
                    && Objects.equals(lexiconName, other.lexiconName)
                    && Objects.equals(objectId, other.objectId)
                    && Objects.equals(totalTermsCount, other.totalTermsCount)
                    && Objects.equals(minimumHits, other.minimumHits)
                    && Objects.equals(scope, other.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, lexiconName, objectId, totalTermsCount, minimumHits, scope);
        }

        @Override
        public String toString() {
            return "Body[id=" + id + ", lexiconName=" + lexiconName + ", objectId=" + objectId
                    + ", totalTermsCount=" + totalTermsCount + ", minimumHits=" + minimumHits
                    + ", scope=" + scope + "]";
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses a raw {@code feature_definition} JSON string.
     *
     * @throws FeatureDefinitionParseException if the JSON is malformed or
     *                                         missing the required {@code body.lexiconName} value —
     *                                         every feature this engine scans with MUST resolve to
     *                                         a {@code .hdb} filename, so a definition without one
     *                                         is treated as an error, not silently skipped
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
        if (parsed.getBody() == null || parsed.getBody().getLexiconName() == null || parsed.getBody().getLexiconName().isBlank()) {
            throw new FeatureDefinitionParseException(
                    "feature_definition JSON is missing required body.lexiconName value: " + rawJson);
        }
        return parsed;
    }

    /**
     * Thrown by {@link #parse} on malformed or incomplete {@code feature_definition} JSON.
     */
    public static final class FeatureDefinitionParseException extends RuntimeException {
        public FeatureDefinitionParseException(String message) {
            super(message);
        }

        public FeatureDefinitionParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
