package com.db.macs3.ecomms.spectre.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the job configuration JSON file loaded from a GCS path at job
 * startup. Per the platform's operating convention, individual BigQuery /
 * view table names are NOT passed as separate {@code --flag value} CLI
 * arguments; instead a single {@code --configGcsPath} argument points to a
 * JSON file (versioned per environment) containing all table references and
 * Spark tuning parameters for that Dataproc job submission.
 *
 * <h2>Example JSON file</h2>
 * <pre>
 * {
 *   "bqProject": "my-gcp-project",
 *   "bqDataset": "spectre_audit_working",
 *   "inputTables": {
 *     "languageFeatureDecision": "spectre-audit.language-feature-decision",
 *     "featureMaster": "spectre-audit.feature-master"
 *   },
 *   "viewName": "v_lexicon_scan_engine_input",
 *   "outputTables": {
 *     "lexiconHitSummary":    "spectre-audit.lexicon-hit-summary",
 *     "lexiconHitRestricted": "spectre-audit.lexicon-hit-restricted",
 *     "featureHitSummary":    "spectre-audit.feature-hit-summary",
 *     "pipelineStageAudit":   "spectre-audit.pipeline_stage_audit",
 *     "pipelineRecordAudit":  "spectre-audit.pipeline_record_audit"
 *   },
 *   "hdbGcsBucket": "spectre-hdb-bucket",
 *   "hdbGcsPrefix": "hyperscan-databases",
 *   "msgGcsBucket": "spectre-messages-bucket",
 *   "msgGcsPrefix": "messages",
 *   "sparkConf": {
 *     "spark.executor.memory": "8g",
 *     "spark.sql.shuffle.partitions": "200"
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("bqProject")
    private String bqProject;

    @JsonProperty("bqDataset")
    private String bqDataset;

    @JsonProperty("inputTables")
    private InputTables inputTables = new InputTables();

    @JsonProperty("viewName")
    private String viewName = "v_lexicon_scan_engine_input";

    @JsonProperty("outputTables")
    private OutputTables outputTables = new OutputTables();

    @JsonProperty("hdbGcsBucket")
    private String hdbGcsBucket;

    @JsonProperty("hdbGcsPrefix")
    private String hdbGcsPrefix;

    @JsonProperty("msgGcsBucket")
    private String msgGcsBucket;

    @JsonProperty("msgGcsPrefix")
    private String msgGcsPrefix;

    /** Optional Spark configuration overrides, applied on top of AppConfig defaults. */
    @JsonProperty("sparkConf")
    private Map<String, String> sparkConf = new HashMap<>();

    // ── Nested: input table references ──────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InputTables implements Serializable {
        @JsonProperty("languageFeatureDecision")
        public String languageFeatureDecision = "spectre-audit.language-feature-decision";

        @JsonProperty("featureMaster")
        public String featureMaster = "spectre-audit.feature-master";
    }

    // ── Nested: output table references ─────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutputTables implements Serializable {
        @JsonProperty("lexiconHitSummary")
        public String lexiconHitSummary = "spectre-audit.lexicon-hit-summary";

        @JsonProperty("lexiconHitRestricted")
        public String lexiconHitRestricted = "spectre-audit.lexicon-hit-restricted";

        @JsonProperty("featureHitSummary")
        public String featureHitSummary = "spectre-audit.feature-hit-summary";

        @JsonProperty("pipelineStageAudit")
        public String pipelineStageAudit = "spectre-audit.pipeline_stage_audit";

        @JsonProperty("pipelineRecordAudit")
        public String pipelineRecordAudit = "spectre-audit.pipeline_record_audit";
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String getBqProject()               { return bqProject; }
    public void setBqProject(String v)          { this.bqProject = v; }
    public String getBqDataset()                { return bqDataset; }
    public void setBqDataset(String v)          { this.bqDataset = v; }
    public InputTables getInputTables()         { return inputTables; }
    public void setInputTables(InputTables v)   { this.inputTables = v; }
    public String getViewName()                 { return viewName; }
    public void setViewName(String v)           { this.viewName = v; }
    public OutputTables getOutputTables()       { return outputTables; }
    public void setOutputTables(OutputTables v) { this.outputTables = v; }
    public String getHdbGcsBucket()             { return hdbGcsBucket; }
    public void setHdbGcsBucket(String v)       { this.hdbGcsBucket = v; }
    public String getHdbGcsPrefix()             { return hdbGcsPrefix; }
    public void setHdbGcsPrefix(String v)       { this.hdbGcsPrefix = v; }
    public String getMsgGcsBucket()             { return msgGcsBucket; }
    public void setMsgGcsBucket(String v)       { this.msgGcsBucket = v; }
    public String getMsgGcsPrefix()             { return msgGcsPrefix; }
    public void setMsgGcsPrefix(String v)       { this.msgGcsPrefix = v; }
    public Map<String, String> getSparkConf()   { return sparkConf; }
    public void setSparkConf(Map<String, String> v) { this.sparkConf = v; }

    /** @return the fully-qualified BigQuery view reference: {@code project.dataset.viewName} */
    public String bqViewRef() {
        return bqProject + "." + bqDataset + "." + viewName;
    }
}
