package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * The {@code spectre.engine.bigquery} section of the {@link DataprocConfig}
 * YAML file — the view this job reads plus every output/audit table it
 * writes.
 *
 * <p><strong>Every {@code bq-output-*}/{@code bq-feature-master}/
 * {@code bq-language-feature-dec} field below is already the FULLY QUALIFIED
 * {@code <project>.<dataset>.<table>} identifier</strong> — unlike this
 * class's previous shape (a separate {@code project_id}/{@code output_dataset}
 * plus bare table names, concatenated at call time via a since-removed
 * {@code fullyQualifiedTable(String)} method). The upstream YAML now supplies
 * the complete identifier for each output table directly (see the sample in
 * {@code RuntimeArgs}/{@code DataprocConfig} class Javadoc), so callers
 * ({@code OutputTableWriter}) use these accessors verbatim as the Spark
 * BigQuery connector's {@code "table"} option — no further concatenation.
 *
 * <p>The VIEW is the one exception: {@code bq-project}/{@code bq-dataset}/
 * {@code bq-view-name} remain three separate fields (matching the YAML), so
 * {@link #fullyQualifiedViewName()} still builds the identifier itself.
 *
 * <p>{@code bq-feature-master} and {@code bq-language-feature-dec} are new
 * fields this YAML shape introduces that no class in this engine currently
 * reads — carried here so the config's full shape round-trips faithfully;
 * wiring them into an actual read path is out of scope for this change.
 *
 * <p>A plain class rather than a record, matching this project's other
 * pre-existing model classes.
 */
public final class BqTableConfig implements Serializable {

    private final String bqProject;
    private final String bqDataset;
    private final String bqViewName;
    private final String bqFeatureMaster;
    private final String bqLanguageFeatureDec;
    private final String bqOutputFeatureHitSummary;
    private final String bqOutputHitSummary;
    private final String bqOutputHitRestricted;
    private final String bqOutputHitUnrestricted;
    private final String bqOutputStageAudit;
    private final String bqOutputRecordAudit;

    /**
     * @param bqProject                     the GCP project {@code bqDataset}/{@code bqViewName} live in
     * @param bqDataset                       BQ dataset containing {@code vw_src_msg_lexicon_decision_mapping}
     * @param bqViewName                      the view's own name
     * @param bqFeatureMaster                  fully-qualified — see class Javadoc "new fields"
     * @param bqLanguageFeatureDec              fully-qualified — see class Javadoc "new fields"
     * @param bqOutputFeatureHitSummary         fully-qualified {@code feature-hit-summary} table
     * @param bqOutputHitSummary                 fully-qualified {@code lexicon-hit-summary} table
     * @param bqOutputHitRestricted               fully-qualified {@code lexicon-hit-restricted} table
     * @param bqOutputHitUnrestricted              fully-qualified {@code lexicon-hit-unrestricted} table
     * @param bqOutputStageAudit                    fully-qualified {@code pipeline-stage-audit} table
     * @param bqOutputRecordAudit                    fully-qualified {@code pipeline-record-audit} table
     */
    @JsonCreator
    public BqTableConfig(@JsonProperty("bq-project") String bqProject,
                          @JsonProperty("bq-dataset") String bqDataset,
                          @JsonProperty("bq-view-name") String bqViewName,
                          @JsonProperty("bq-feature-master") String bqFeatureMaster,
                          @JsonProperty("bq-language-feature-dec") String bqLanguageFeatureDec,
                          @JsonProperty("bq-output-feature-hit-summary") String bqOutputFeatureHitSummary,
                          @JsonProperty("bq-output-hit-summary") String bqOutputHitSummary,
                          @JsonProperty("bq-output-hit-restricted") String bqOutputHitRestricted,
                          @JsonProperty("bq-output-hit-unrestricted") String bqOutputHitUnrestricted,
                          @JsonProperty("bq-output-stage-audit") String bqOutputStageAudit,
                          @JsonProperty("bq-output-record-audit") String bqOutputRecordAudit) {
        this.bqProject = bqProject;
        this.bqDataset = bqDataset;
        this.bqViewName = bqViewName;
        this.bqFeatureMaster = bqFeatureMaster;
        this.bqLanguageFeatureDec = bqLanguageFeatureDec;
        this.bqOutputFeatureHitSummary = bqOutputFeatureHitSummary;
        this.bqOutputHitSummary = bqOutputHitSummary;
        this.bqOutputHitRestricted = bqOutputHitRestricted;
        this.bqOutputHitUnrestricted = bqOutputHitUnrestricted;
        this.bqOutputStageAudit = bqOutputStageAudit;
        this.bqOutputRecordAudit = bqOutputRecordAudit;
    }

    public String bqProject() { return bqProject; }
    public String bqDataset() { return bqDataset; }
    public String bqViewName() { return bqViewName; }
    public String bqFeatureMaster() { return bqFeatureMaster; }
    public String bqLanguageFeatureDec() { return bqLanguageFeatureDec; }
    public String bqOutputFeatureHitSummary() { return bqOutputFeatureHitSummary; }
    public String bqOutputHitSummary() { return bqOutputHitSummary; }
    public String bqOutputHitRestricted() { return bqOutputHitRestricted; }
    public String bqOutputHitUnrestricted() { return bqOutputHitUnrestricted; }
    public String bqOutputStageAudit() { return bqOutputStageAudit; }
    public String bqOutputRecordAudit() { return bqOutputRecordAudit; }

    /** {@code <bq-project>.<bq-dataset>.<bq-view-name>} — the fully-qualified identifier the Spark BQ connector expects. */
    public String fullyQualifiedViewName() {
        return bqProject + "." + bqDataset + "." + bqViewName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BqTableConfig)) {
            return false;
        }
        BqTableConfig other = (BqTableConfig) o;
        return Objects.equals(bqProject, other.bqProject) && Objects.equals(bqDataset, other.bqDataset)
                && Objects.equals(bqViewName, other.bqViewName)
                && Objects.equals(bqFeatureMaster, other.bqFeatureMaster)
                && Objects.equals(bqLanguageFeatureDec, other.bqLanguageFeatureDec)
                && Objects.equals(bqOutputFeatureHitSummary, other.bqOutputFeatureHitSummary)
                && Objects.equals(bqOutputHitSummary, other.bqOutputHitSummary)
                && Objects.equals(bqOutputHitRestricted, other.bqOutputHitRestricted)
                && Objects.equals(bqOutputHitUnrestricted, other.bqOutputHitUnrestricted)
                && Objects.equals(bqOutputStageAudit, other.bqOutputStageAudit)
                && Objects.equals(bqOutputRecordAudit, other.bqOutputRecordAudit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bqProject, bqDataset, bqViewName, bqFeatureMaster, bqLanguageFeatureDec,
                bqOutputFeatureHitSummary, bqOutputHitSummary, bqOutputHitRestricted, bqOutputHitUnrestricted,
                bqOutputStageAudit, bqOutputRecordAudit);
    }

    @Override
    public String toString() {
        return "BqTableConfig[bqProject=" + bqProject + ", bqDataset=" + bqDataset + ", bqViewName=" + bqViewName
                + ", bqFeatureMaster=" + bqFeatureMaster + ", bqLanguageFeatureDec=" + bqLanguageFeatureDec
                + ", bqOutputFeatureHitSummary=" + bqOutputFeatureHitSummary
                + ", bqOutputHitSummary=" + bqOutputHitSummary
                + ", bqOutputHitRestricted=" + bqOutputHitRestricted
                + ", bqOutputHitUnrestricted=" + bqOutputHitUnrestricted
                + ", bqOutputStageAudit=" + bqOutputStageAudit
                + ", bqOutputRecordAudit=" + bqOutputRecordAudit + "]";
    }
}
