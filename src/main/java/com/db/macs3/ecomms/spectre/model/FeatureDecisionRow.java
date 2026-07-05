package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents one row from the pre-computed BigQuery view joining
 * {@code spectre-audit.language-feature-decision} (LHS) with
 * {@code spectre-audit.feature-master} (RHS).
 *
 * <h2>View extraction rules</h2>
 * <p>The view flattens {@code language-feature-decision.features} (REPEATED)
 * and, for composite features, further flattens {@code features.sub_feature}
 * (REPEATED) — but ONLY sub-features where {@code sub_feature.type = 'lexicon'}.
 * Non-lexicon sub-features (e.g. {@code metadata}, {@code evaluation}) are
 * dropped entirely; they are out of scope for the Lexicon Scan Engine and are
 * evaluated by other services.
 *
 * <ul>
 *   <li>{@code features.type = 'lexicon'} → one row emitted;
 *       {@link #getLexiconName()} = {@code features.name}</li>
 *   <li>{@code features.type = 'composite'} → UNNEST {@code sub_feature}
 *       WHERE {@code sub_feature.type = 'lexicon'} → one row PER qualifying
 *       sub-feature; {@link #getLexiconName()} = {@code sub_feature.name}.
 *       {@link #getFeatureId()} / {@link #getFeatureName()} / {@link #getFeatureOperator()} /
 *       {@link #isNoiseReduction()} remain the PARENT composite's values —
 *       these identify which composite this lexicon term belongs to, which
 *       is required for correct {@code feature-hit-summary} reporting.</li>
 * </ul>
 *
 * <p>The same extraction pattern applies on the {@code feature-master} side to
 * resolve {@link #getFmFeatureDefinition()}: direct lexicon features use
 * {@code feature_definition}; composite features use
 * {@code sub_feature.definition} WHERE {@code sub_feature.type = 'lexicon'}.
 *
 * <p>The join key is {@code language-feature-decision.process_id =
 * feature-master.policy_engine_id AND <resolved lexicon name matches on both sides>}.
 */
public class FeatureDecisionRow implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── From language-feature-decision (LHS) — all columns per spec ──────────
    private String messageId;
    private String runDate;
    private String processId;
    private String pipelineExecId;
    private String sentDate;
    private String messageType;      // "unrestricted" | "restricted"

    // ── Parent feature identity (the row as it appears in features[]) ────────
    /** Parent feature id — STRING per BQ schema. Stable across sub-feature expansion. */
    private String featureId;

    /** Parent feature type: "lexicon" or "composite" (lowercase per schema). */
    private String featureType;

    /** Parent feature name (e.g. "NotNewsLetter" for a composite, or the direct lexicon name). */
    private String featureName;

    /** Parent feature operator ("OR" / "AND"), used for noise-reduction evaluation across composites. */
    private String featureOperator;

    /** Parent feature's is_noise_reduction flag as the raw STRING "Y"/"N" from BQ. */
    private String isNoiseReductionRaw;

    // ── Resolved lexicon identity for THIS row (may be the parent itself, or a sub-feature) ──
    /**
     * The lexicon feature/sub-feature name to load a .hdb for.
     * Equal to {@link #featureName} when {@code featureType='lexicon'}, or
     * equal to the qualifying {@code sub_feature.name} when {@code featureType='composite'}.
     */
    private String lexiconName;

    /**
     * {@code true} when this row came from a composite's sub_feature expansion
     * (as opposed to being a direct standalone lexicon feature).
     */
    private boolean fromComposite;

    // ── From feature-master (RHS) ─────────────────────────────────────────────
    /** Resolved feature/sub-feature definition JSON (informational; not required for scanning itself). */
    private String fmFeatureDefinition;

    public FeatureDecisionRow() {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static FeatureDecisionRow of(String messageId, String runDate, String processId,
                                         String pipelineExecId, String sentDate, String messageType,
                                         String featureId, String featureType, String featureName,
                                         String featureOperator, String isNoiseReductionRaw,
                                         String lexiconName, boolean fromComposite,
                                         String fmFeatureDefinition) {
        FeatureDecisionRow row = new FeatureDecisionRow();
        row.messageId            = messageId;
        row.runDate              = runDate;
        row.processId            = processId;
        row.pipelineExecId       = pipelineExecId;
        row.sentDate             = sentDate;
        row.messageType          = messageType;
        row.featureId            = featureId;
        row.featureType          = featureType;
        row.featureName          = featureName;
        row.featureOperator      = featureOperator;
        row.isNoiseReductionRaw  = isNoiseReductionRaw;
        row.lexiconName          = lexiconName;
        row.fromComposite        = fromComposite;
        row.fmFeatureDefinition  = fmFeatureDefinition;
        return row;
    }

    /** @return {@code true} when {@link #isNoiseReductionRaw} is "Y" (case-insensitive). */
    public boolean isNoiseReduction() {
        return "Y".equalsIgnoreCase(isNoiseReductionRaw);
    }

    /** @return {@code true} when {@link #messageType} is "restricted" (case-insensitive). */
    public boolean isRestrictedMessage() {
        return "restricted".equalsIgnoreCase(messageType);
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getMessageId()               { return messageId; }
    public void setMessageId(String v)         { this.messageId = v; }
    public String getRunDate()                  { return runDate; }
    public void setRunDate(String v)           { this.runDate = v; }
    public String getProcessId()                { return processId; }
    public void setProcessId(String v)         { this.processId = v; }
    public String getPipelineExecId()           { return pipelineExecId; }
    public void setPipelineExecId(String v)    { this.pipelineExecId = v; }
    public String getSentDate()                 { return sentDate; }
    public void setSentDate(String v)          { this.sentDate = v; }
    public String getMessageType()              { return messageType; }
    public void setMessageType(String v)       { this.messageType = v; }
    public String getFeatureId()                { return featureId; }
    public void setFeatureId(String v)         { this.featureId = v; }
    public String getFeatureType()              { return featureType; }
    public void setFeatureType(String v)       { this.featureType = v; }
    public String getFeatureName()              { return featureName; }
    public void setFeatureName(String v)       { this.featureName = v; }
    public String getFeatureOperator()          { return featureOperator; }
    public void setFeatureOperator(String v)   { this.featureOperator = v; }
    public String getIsNoiseReductionRaw()      { return isNoiseReductionRaw; }
    public void setIsNoiseReductionRaw(String v){ this.isNoiseReductionRaw = v; }
    public String getLexiconName()              { return lexiconName; }
    public void setLexiconName(String v)       { this.lexiconName = v; }
    public boolean isFromComposite()            { return fromComposite; }
    public void setFromComposite(boolean v)    { this.fromComposite = v; }
    public String getFmFeatureDefinition()             { return fmFeatureDefinition; }
    public void setFmFeatureDefinition(String v)       { this.fmFeatureDefinition = v; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FeatureDecisionRow that = (FeatureDecisionRow) o;
        return Objects.equals(messageId, that.messageId) &&
               Objects.equals(featureId, that.featureId) &&
               Objects.equals(lexiconName, that.lexiconName);
    }

    @Override
    public int hashCode() { return Objects.hash(messageId, featureId, lexiconName); }

    @Override
    public String toString() {
        return "FeatureDecisionRow{messageId='" + messageId + "', featureId='" + featureId +
               "', featureName='" + featureName + "', lexiconName='" + lexiconName +
               "', fromComposite=" + fromComposite + ", isNR=" + isNoiseReduction() + '}';
    }
}
