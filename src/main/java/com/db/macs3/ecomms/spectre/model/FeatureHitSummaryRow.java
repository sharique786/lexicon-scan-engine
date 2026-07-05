package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * One row in the {@code spectre-audit.feature-hit-summary} BigQuery output
 * table — a FLAT (non-nested) table shared across multiple Dataproc jobs in
 * the eCOMMS platform, each contributing rows for the feature types it owns.
 *
 * <h2>Scope of the Lexicon Scan Engine's contribution</h2>
 * <p>This engine writes rows ONLY for the feature/sub-feature combinations it
 * actually evaluates via Hyperscan:
 * <ul>
 *   <li>Direct {@code feature_type='lexicon'} features → one row per
 *       (message, feature), with {@link #subFeatureType} / {@link #subFeatureName}
 *       left null.</li>
 *   <li>{@code feature_type='composite'} features → one row per
 *       (message, composite feature, lexicon sub-feature), with
 *       {@link #featureId} / {@link #featureName} / {@link #featureIsNoiseReduction}
 *       carrying the PARENT composite's identity and {@link #subFeatureType} =
 *       {@code "lexicon"}, {@link #subFeatureName} = the specific sub-feature name.</li>
 * </ul>
 * <p>Non-lexicon sub-features within a composite (e.g. {@code metadata},
 * {@code evaluation}) are OUT OF SCOPE for this engine — those rows, if
 * present in the table, are written by the services that own those feature
 * types (e.g. a Metadata Tagging Engine), not by the Lexicon Scan Engine.
 *
 * <p>Schema (BQ):
 * <pre>
 * message_id                  STRING NOT NULL
 * run_date                    STRING
 * process_id                  STRING NOT NULL
 * pipeline_exec_id            STRING NOT NULL
 * sent_date                   STRING
 * message_type                STRING
 * feature_id                  STRING
 * feature_name                STRING
 * feature_type                STRING   ("lexicon" | "composite")
 * feature_is_noise_reduction  STRING   ("Y" | "N")
 * sub_feature_name            STRING   (null for direct lexicon features)
 * sub_feature_type            STRING   ("lexicon", or null for direct features)
 * hit_status                  STRING   ("Yes" | "No")
 * created_by                  STRING
 * created_ts                  TIMESTAMP
 * </pre>
 */
public class FeatureHitSummaryRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String HIT_YES = "Yes";
    public static final String HIT_NO  = "No";

    private String messageId;
    private String runDate;
    private String processId;
    private String pipelineExecId;
    private String sentDate;
    private String messageType;
    private String featureId;
    private String featureName;
    private String featureType;
    private String featureIsNoiseReduction;
    private String subFeatureName;
    private String subFeatureType;
    private String hitStatus;
    private String createdBy = "SYSTEM";
    private Timestamp createdTs;

    public FeatureHitSummaryRow() {}

    public String getMessageId()                       { return messageId; }
    public void setMessageId(String v)                 { this.messageId = v; }
    public String getRunDate()                          { return runDate; }
    public void setRunDate(String v)                   { this.runDate = v; }
    public String getProcessId()                        { return processId; }
    public void setProcessId(String v)                 { this.processId = v; }
    public String getPipelineExecId()                   { return pipelineExecId; }
    public void setPipelineExecId(String v)            { this.pipelineExecId = v; }
    public String getSentDate()                         { return sentDate; }
    public void setSentDate(String v)                  { this.sentDate = v; }
    public String getMessageType()                      { return messageType; }
    public void setMessageType(String v)               { this.messageType = v; }
    public String getFeatureId()                        { return featureId; }
    public void setFeatureId(String v)                 { this.featureId = v; }
    public String getFeatureName()                      { return featureName; }
    public void setFeatureName(String v)               { this.featureName = v; }
    public String getFeatureType()                      { return featureType; }
    public void setFeatureType(String v)               { this.featureType = v; }
    public String getFeatureIsNoiseReduction()          { return featureIsNoiseReduction; }
    public void setFeatureIsNoiseReduction(String v)   { this.featureIsNoiseReduction = v; }
    public String getSubFeatureName()                   { return subFeatureName; }
    public void setSubFeatureName(String v)            { this.subFeatureName = v; }
    public String getSubFeatureType()                   { return subFeatureType; }
    public void setSubFeatureType(String v)            { this.subFeatureType = v; }
    public String getHitStatus()                        { return hitStatus; }
    public void setHitStatus(String v)                 { this.hitStatus = v; }
    public String getCreatedBy()                        { return createdBy; }
    public void setCreatedBy(String v)                 { this.createdBy = v; }
    public Timestamp getCreatedTs()                     { return createdTs; }
    public void setCreatedTs(Timestamp v)              { this.createdTs = v; }

    @Override
    public String toString() {
        return "FeatureHitSummaryRow{messageId='" + messageId + "', featureName='" + featureName +
               "', subFeatureName='" + subFeatureName + "', hitStatus='" + hitStatus + "'}";
    }
}
