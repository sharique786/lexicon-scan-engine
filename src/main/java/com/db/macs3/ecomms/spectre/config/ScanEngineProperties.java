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

    /**
     * Range-partition messages by their feature set (with {@code message_id} as tie-breaker) before scanning, so
     * each partition/executor needs far fewer Hyperscan bundles. Costs one extra shuffle of the joined messages
     * (attachments included) — off by default; see {@code FeatureSetLayout}. Pure layout — no effect on results.
     */
    private boolean partitionByFeatureSet = false;

    public boolean isPartitionByFeatureSet() {
        return partitionByFeatureSet;
    }

    public void setPartitionByFeatureSet(boolean partitionByFeatureSet) {
        this.partitionByFeatureSet = partitionByFeatureSet;
    }

    /**
     * Share loaded Hyperscan bundles between all tasks of one executor (see {@code SharedBundleCache}) instead of
     * every task loading its own copy. False restores per-partition private loading.
     */
    private boolean sharedBundleCacheEnabled = true;

    /**
     * How many bundles NO running task currently uses an executor keeps open for the next task. Each costs native
     * (off-heap) memory — budget it in {@code spark.executor.memoryOverhead}. Only used when the shared cache is on.
     */
    private int maxSharedIdleBundlesPerExecutor = 32;

    /**
     * Maximum number of characters a disclaimer-style term ending in {@code .*} is extended by after its matched
     * prefix (the tail otherwise runs to the end of the line) — see {@code CollapsingScanner}.
     */
    private int maxTailChars = com.gliwka.hyperscan.wrapper.CollapsingScanner.DEFAULT_MAX_TAIL_CHARS;

    public int getMaxTailChars() {
        return maxTailChars;
    }

    public void setMaxTailChars(int maxTailChars) {
        this.maxTailChars = maxTailChars;
    }

    public boolean isSharedBundleCacheEnabled() {
        return sharedBundleCacheEnabled;
    }

    public void setSharedBundleCacheEnabled(boolean sharedBundleCacheEnabled) {
        this.sharedBundleCacheEnabled = sharedBundleCacheEnabled;
    }

    public int getMaxSharedIdleBundlesPerExecutor() {
        return maxSharedIdleBundlesPerExecutor;
    }

    public void setMaxSharedIdleBundlesPerExecutor(int maxSharedIdleBundlesPerExecutor) {
        this.maxSharedIdleBundlesPerExecutor = maxSharedIdleBundlesPerExecutor;
    }

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
