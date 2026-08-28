package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * Input/output BigQuery view and table identifiers — requirement 4.b: for a
 * real Dataproc submission, this is read from a JSON file on GCS (path
 * passed as a Dataproc submit argument), not hard-coded or Spring-bound;
 * requirement 4.e: for tests, an equivalent shape is supplied via the
 * {@code test} Spring profile's application properties instead (see
 * {@code ScanEngineProperties}) rather than requiring a real GCS file.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class BqTableConfig implements Serializable {

    private final String projectId;
    private final String viewDataset;
    private final String viewName;
    private final String outputDataset;
    private final String lexiconHitSummaryTable;
    private final String lexiconHitRestrictedTable;
    private final String lexiconHitUnrestrictedTable;
    private final String featureHitSummaryTable;
    private final String pipelineStageAuditTable;
    private final String pipelineRecordAuditTable;

    /**
     * @param projectId                     the GCP project every table/view below lives in
     * @param viewDataset                     BQ dataset containing {@code vw_src_msg_lexicon_decision_mapping}
     * @param viewName                        the view's own name
     * @param outputDataset                   BQ dataset every output/audit table below lives in
     *                                        (all six currently share one dataset — {@code spectre-audit} —
     *                                        but each is named separately here rather than assuming that
     *                                        stays true)
     */
    @JsonCreator
    public BqTableConfig(@JsonProperty("project_id") String projectId,
                          @JsonProperty("view_dataset") String viewDataset,
                          @JsonProperty("view_name") String viewName,
                          @JsonProperty("output_dataset") String outputDataset,
                          @JsonProperty("lexicon_hit_summary_table") String lexiconHitSummaryTable,
                          @JsonProperty("lexicon_hit_restricted_table") String lexiconHitRestrictedTable,
                          @JsonProperty("lexicon_hit_unrestricted_table") String lexiconHitUnrestrictedTable,
                          @JsonProperty("feature_hit_summary_table") String featureHitSummaryTable,
                          @JsonProperty("pipeline_stage_audit_table") String pipelineStageAuditTable,
                          @JsonProperty("pipeline_record_audit_table") String pipelineRecordAuditTable) {
        this.projectId = projectId;
        this.viewDataset = viewDataset;
        this.viewName = viewName;
        this.outputDataset = outputDataset;
        this.lexiconHitSummaryTable = lexiconHitSummaryTable;
        this.lexiconHitRestrictedTable = lexiconHitRestrictedTable;
        this.lexiconHitUnrestrictedTable = lexiconHitUnrestrictedTable;
        this.featureHitSummaryTable = featureHitSummaryTable;
        this.pipelineStageAuditTable = pipelineStageAuditTable;
        this.pipelineRecordAuditTable = pipelineRecordAuditTable;
    }

    public String projectId() { return projectId; }
    public String viewDataset() { return viewDataset; }
    public String viewName() { return viewName; }
    public String outputDataset() { return outputDataset; }
    public String lexiconHitSummaryTable() { return lexiconHitSummaryTable; }
    public String lexiconHitRestrictedTable() { return lexiconHitRestrictedTable; }
    public String lexiconHitUnrestrictedTable() { return lexiconHitUnrestrictedTable; }
    public String featureHitSummaryTable() { return featureHitSummaryTable; }
    public String pipelineStageAuditTable() { return pipelineStageAuditTable; }
    public String pipelineRecordAuditTable() { return pipelineRecordAuditTable; }

    /** {@code <project>.<dataset>.<view>} — the fully-qualified identifier the Spark BQ connector expects. */
    public String fullyQualifiedViewName() {
        return projectId + "." + viewDataset + "." + viewName;
    }

    public String fullyQualifiedTable(String tableName) {
        return projectId + "." + outputDataset + "." + tableName;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Parses the JSON file's content — see class Javadoc for where this file lives in production. */
    public static BqTableConfig parse(InputStream jsonStream) throws IOException {
        return MAPPER.readValue(
                new String(jsonStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8), BqTableConfig.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BqTableConfig)) return false;
        BqTableConfig other = (BqTableConfig) o;
        return Objects.equals(projectId, other.projectId) && Objects.equals(viewDataset, other.viewDataset)
                && Objects.equals(viewName, other.viewName) && Objects.equals(outputDataset, other.outputDataset)
                && Objects.equals(lexiconHitSummaryTable, other.lexiconHitSummaryTable)
                && Objects.equals(lexiconHitRestrictedTable, other.lexiconHitRestrictedTable)
                && Objects.equals(lexiconHitUnrestrictedTable, other.lexiconHitUnrestrictedTable)
                && Objects.equals(featureHitSummaryTable, other.featureHitSummaryTable)
                && Objects.equals(pipelineStageAuditTable, other.pipelineStageAuditTable)
                && Objects.equals(pipelineRecordAuditTable, other.pipelineRecordAuditTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, viewDataset, viewName, outputDataset, lexiconHitSummaryTable,
                lexiconHitRestrictedTable, lexiconHitUnrestrictedTable, featureHitSummaryTable,
                pipelineStageAuditTable, pipelineRecordAuditTable);
    }

    @Override
    public String toString() {
        return "BqTableConfig[projectId=" + projectId + ", viewDataset=" + viewDataset + ", viewName=" + viewName
                + ", outputDataset=" + outputDataset + ", lexiconHitSummaryTable=" + lexiconHitSummaryTable
                + ", lexiconHitRestrictedTable=" + lexiconHitRestrictedTable
                + ", lexiconHitUnrestrictedTable=" + lexiconHitUnrestrictedTable
                + ", featureHitSummaryTable=" + featureHitSummaryTable
                + ", pipelineStageAuditTable=" + pipelineStageAuditTable
                + ", pipelineRecordAuditTable=" + pipelineRecordAuditTable + "]";
    }
}
