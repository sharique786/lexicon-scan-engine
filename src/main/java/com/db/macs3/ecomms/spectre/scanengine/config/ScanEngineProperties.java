package com.db.macs3.ecomms.spectre.scanengine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring-bound configuration — everything NOT supplied per-invocation by
 * Airflow ({@link RuntimeArgs}) or the {@link DataprocConfig} YAML it points
 * to via {@code config_file_path} (GCS buckets, BQ table/view identifiers —
 * see that class). Bound from {@code application.yml} in production and
 * {@code application-test.yml} for tests.
 *
 * <p>The GCS bucket fields this class used to carry ({@code live-message-bucket},
 * {@code test-message-bucket}, {@code environment-bucket}) and
 * {@code bq-table-config-path} moved to {@link DataprocConfig} when Composer
 * switched from two GCS JSON file paths to 7 {@code --key=value} arguments
 * plus one YAML config file — see {@link RuntimeArgs} class Javadoc for the
 * full picture. What remains here is genuinely job-infrastructure config,
 * not something a specific pipeline run supplies.
 *
 * <h2>{@link #maxAttachmentSizeBytes}</h2>
 * <p>Bound via {@code @Value} from the environment variable
 * {@code SPECTRE_MAX_ATTACHMENT_SIZE_BYTES} directly — NOT via this class's
 * own {@code @ConfigurationProperties} relaxed binding, since
 * {@code SPECTRE_...} does not follow the {@code scan-engine.*} prefix's
 * naming convention (relaxed binding would expect
 * {@code SCAN_ENGINE_MAX_ATTACHMENT_SIZE_BYTES}). {@code null} (the default
 * when the env var is absent) means no limit — every attachment is scanned
 * regardless of size.
 */
@ConfigurationProperties(prefix = "scan-engine")
public class ScanEngineProperties {

    private static final String DEFAULT_STAGE_IDENTITY = "lexicon-scan-engine";

    /** See class Javadoc — bound via {@code @Value}, not this class's own relaxed binding. */
    @Value("${SPECTRE_MAX_ATTACHMENT_SIZE_BYTES:#{null}}")
    private Long maxAttachmentSizeBytes;

    /** Bounds each Spark partition's cached-bundle count (database + term metadata together) — see {@code HyperscanBundleLoader}. */
    private int maxCachedDatabasesPerPartition = 20;

    /** The identity written to every output/audit row's {@code created_by} column. */
    private String createdBy = DEFAULT_STAGE_IDENTITY;

    /** This job's {@code pipeline_stage_audit.stage_name} identity. */
    private String stageName = DEFAULT_STAGE_IDENTITY;

    public Long getMaxAttachmentSizeBytes() { return maxAttachmentSizeBytes; }
    public void setMaxAttachmentSizeBytes(Long maxAttachmentSizeBytes) { this.maxAttachmentSizeBytes = maxAttachmentSizeBytes; }

    public int getMaxCachedDatabasesPerPartition() { return maxCachedDatabasesPerPartition; }
    public void setMaxCachedDatabasesPerPartition(int maxCachedDatabasesPerPartition) { this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public String getStageName() { return stageName; }
    public void setStageName(String stageName) { this.stageName = stageName; }
}
