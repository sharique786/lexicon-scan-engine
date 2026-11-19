package com.db.macs3.ecomms.spectre.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring-bound job-infrastructure configuration — everything NOT supplied per invocation by
 * {@link RuntimeArgs} or the {@link DataprocConfig} YAML. Bound from {@code application.yml} in
 * production and {@code application-test.yml} for tests.
 *
 * <p>{@code maxAttachmentSizeBytes} and {@code stageName} used to live here (bound via
 * {@code SPECTRE_MAX_ATTACHMENT_SIZE_BYTES} and {@code stage-name} respectively) but are now
 * sourced from the {@link DataprocConfig} YAML instead — {@code spectre.engine.messages.max-attachment-limit}
 * ({@link DataprocConfig.MessagesGcsConfig#maxAttachmentLimit()}) and {@code spectre.engine.stage-name}
 * ({@link DataprocConfig#stageName()}) — since both are per-pipeline-execution values, not
 * job-infrastructure config. {@code null}/absent still means "no limit" for the attachment size, same as
 * before.
 */
@ConfigurationProperties(prefix = "scan-engine")
public class ScanEngineProperties {

    /**
     * Bounds each Spark partition's cached-bundle count (database + term metadata together) — see {@code HyperscanBundleLoader}.
     */
    @Value("${max-cached-databases-per-partition:20}")
    private int maxCachedDatabasesPerPartition;

    /**
     * The identity written to every output/audit row's {@code created_by} column.
     */
    @Value("${created-by: SPECTRE-COMPOSER-SA}")
    private String createdBy;

    /**
     * Sort each partition's messages by their feature set before scanning, so messages needing the same
     * Hyperscan bundles run back to back (see {@code ScanEngineJobRunner}). Pure ordering — no effect on results.
     */
    @Value("${sort-partitions-by-feature-set:true}")
    private boolean sortPartitionsByFeatureSet = true;

    public boolean isSortPartitionsByFeatureSet() {
        return sortPartitionsByFeatureSet;
    }

    public void setSortPartitionsByFeatureSet(boolean sortPartitionsByFeatureSet) {
        this.sortPartitionsByFeatureSet = sortPartitionsByFeatureSet;
    }

    public int getMaxCachedDatabasesPerPartition() {
        return maxCachedDatabasesPerPartition;
    }

    public void setMaxCachedDatabasesPerPartition(int maxCachedDatabasesPerPartition) {
        this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
}
