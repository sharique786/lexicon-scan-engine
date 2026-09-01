package com.db.macs3.ecomms.spectre.scanengine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring-bound configuration — everything NOT supplied per-invocation by
 * Airflow ({@link RuntimeArgs}) or read from the GCS table-config JSON
 * ({@link BqTableConfig}). Bound from {@code application.yml} in production
 * and {@code application-test.yml} for tests.
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

    /** GCS bucket for AVRO messages under the {@code policy-alert-live} workflow. */
    private String liveMessageBucket;

    /** GCS bucket for AVRO messages under the {@code policy-alert-test} workflow. */
    private String testMessageBucket;

    /**
     * GCS bucket used for the Hyperscan zip-bundle path template
     * ({@code gs://<environment_bkt>/policy_test/...}) and for the
     * {@code lexicon-hit-restricted} CSV mirror output. May be the same
     * physical bucket as {@link #liveMessageBucket}/{@link #testMessageBucket}
     * depending on environment, but is configured independently.
     */
    private String environmentBucket;

    /** See class Javadoc — bound via {@code @Value}, not this class's own relaxed binding. */
    @Value("${SPECTRE_MAX_ATTACHMENT_SIZE_BYTES:#{null}}")
    private Long maxAttachmentSizeBytes;

    /** Bounds each Spark partition's cached-bundle count (database + term metadata together) — see {@code HyperscanBundleLoader}. */
    private int maxCachedDatabasesPerPartition = 20;

    /** The identity written to every output/audit row's {@code created_by} column. */
    private String createdBy = DEFAULT_STAGE_IDENTITY;

    /** This job's {@code pipeline_stage_audit.stage_name} identity. */
    private String stageName = DEFAULT_STAGE_IDENTITY;

    /** GCS path to the {@link BqTableConfig} JSON file — a Dataproc submit argument in production. */
    private String bqTableConfigPath;

    public String getLiveMessageBucket() { return liveMessageBucket; }
    public void setLiveMessageBucket(String liveMessageBucket) { this.liveMessageBucket = liveMessageBucket; }

    public String getTestMessageBucket() { return testMessageBucket; }
    public void setTestMessageBucket(String testMessageBucket) { this.testMessageBucket = testMessageBucket; }

    public String getEnvironmentBucket() { return environmentBucket; }
    public void setEnvironmentBucket(String environmentBucket) { this.environmentBucket = environmentBucket; }

    public Long getMaxAttachmentSizeBytes() { return maxAttachmentSizeBytes; }
    public void setMaxAttachmentSizeBytes(Long maxAttachmentSizeBytes) { this.maxAttachmentSizeBytes = maxAttachmentSizeBytes; }

    public int getMaxCachedDatabasesPerPartition() { return maxCachedDatabasesPerPartition; }
    public void setMaxCachedDatabasesPerPartition(int maxCachedDatabasesPerPartition) { this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public String getStageName() { return stageName; }
    public void setStageName(String stageName) { this.stageName = stageName; }

    public String getBqTableConfigPath() { return bqTableConfigPath; }
    public void setBqTableConfigPath(String bqTableConfigPath) { this.bqTableConfigPath = bqTableConfigPath; }

    /** @return the correct message bucket for {@code runtimeArgs}' trigger type. */
    public String resolveMessageBucket(RuntimeArgs runtimeArgs) {
        if (runtimeArgs.isLive()) {
            return liveMessageBucket;
        }
        if (runtimeArgs.isTest()) {
            return testMessageBucket;
        }
        throw new IllegalArgumentException(
                "Unrecognised trigger_type '" + runtimeArgs.triggerType() + "' — expected '"
                + RuntimeArgs.TRIGGER_TYPE_LIVE + "' or '" + RuntimeArgs.TRIGGER_TYPE_TEST + "'.");
    }
}
