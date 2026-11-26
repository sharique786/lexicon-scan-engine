package com.db.macs3.ecomms.spectre.config;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring-bound job-infrastructure configuration — everything NOT supplied per invocation by
 * {@link RuntimeArgs} or the {@link DataprocConfig} YAML. Bound from the {@code scan-engine:} block of
 * {@code application.yml} in production and {@code application-test.yml} for tests.
 *
 * <p>Every property is bound the same way: JavaBean binding through its setter, with Spring's relaxed names
 * ({@code max-tail-chars} -> {@link #setMaxTailChars}). The field initialisers are the defaults used when a key
 * is absent. Do NOT add {@code @Value} to these fields: {@code @Value} ignores the {@code scan-engine} prefix
 * (so {@code ${max-tail-chars}} looks up a top-level key that does not exist) and only ever produced the default.
 * {@code ScanEnginePropertiesTest} binds the real {@code application.yml} and checks every key.
 *
 * <p>{@code maxAttachmentSizeBytes} and {@code stageName} used to live here but are now sourced from the
 * {@link DataprocConfig} YAML instead — {@code spectre.engine.messages.max-attachment-limit}
 * ({@link DataprocConfig.MessagesGcsConfig#maxAttachmentLimit()}) and {@code spectre.engine.stage-name}
 * ({@link DataprocConfig#stageName()}) — since both are per-pipeline-execution values.
 */
@ConfigurationProperties(prefix = "scan-engine")
public class ScanEngineProperties {

    /** {@code max-cached-databases-per-partition}: bundles each partition's own LRU holds — see {@code HyperscanBundleLoader}. */
    private int maxCachedDatabasesPerPartition = 20;

    /**
     * {@code shared-bundle-cache-enabled}: share loaded Hyperscan bundles between all tasks of one executor (see
     * {@code SharedBundleCache}) instead of every task loading its own copy. False restores per-partition loading.
     */
    private boolean sharedBundleCacheEnabled = true;

    /**
     * {@code max-shared-idle-bundles-per-executor}: bundles NO running task uses that an executor keeps open for the
     * next task. Each costs native (off-heap) memory — budget it in {@code spark.executor.memoryOverhead}. Only used
     * when the shared cache is on.
     */
    private int maxSharedIdleBundlesPerExecutor = 32;

    /**
     * {@code sort-partitions-by-feature-set}: sort each partition's messages by their feature set before scanning, so
     * messages needing the same bundles run back to back. Pure ordering — no effect on results.
     */
    private boolean sortPartitionsByFeatureSet = true;

    /**
     * {@code partition-by-feature-set}: range-partition messages by feature set (with {@code message_id} as
     * tie-breaker) before scanning. Costs one extra shuffle of the joined messages — off by default; see
     * {@code FeatureSetLayout}. Pure layout — no effect on results.
     */
    private boolean partitionByFeatureSet = false;

    /**
     * {@code max-tail-chars}: max characters a term ending in {@code .*} (e.g. a disclaimer) is extended by after its
     * matched prefix; the tail otherwise stops at the end of the line — see {@code CollapsingScanner}.
     */
    private int maxTailChars = CollapsingScanner.DEFAULT_MAX_TAIL_CHARS;

    /** {@code created-by}: the identity written to every output/audit row's {@code created_by} column. */
    private String createdBy = "SPECTRE-COMPOSER-SA";

    /**
     * {@code auto-size-bundle-cache}: raise {@code max-cached-databases-per-partition} and
     * {@code max-shared-idle-bundles-per-executor} to the run's largest per-message feature count when that is
     * larger, so a message's bundles never evict each other (see {@code BundleCacheSizing}).
     */
    private boolean autoSizeBundleCache = true;

    /** Target rows per written partition (one staged Avro file) for lexicon-hit-summary and feature-hit-summary. */
    private int bqSummaryRowsPerWritePartition = 50_000;

    /** Target rows per written partition for lexicon-hit-restricted/-unrestricted (larger rows: matched text). */
    private int bqDetailRowsPerWritePartition = 20_000;

    /** Target rows per written partition (one Storage Write API stream) for pipeline_record_audit. */
    private int bqAuditRowsPerWritePartition = 250_000;

    /**
     * Rows above which a message-scale table is written as several load jobs; 0 = always one load job (atomic per
     * table). See {@code BqWritePlan} for the partial-commit trade-off.
     */
    private long bqMaxRowsPerLoadJob = 0;

    public boolean isAutoSizeBundleCache() {
        return autoSizeBundleCache;
    }

    public void setAutoSizeBundleCache(boolean autoSizeBundleCache) {
        this.autoSizeBundleCache = autoSizeBundleCache;
    }

    public int getBqSummaryRowsPerWritePartition() {
        return bqSummaryRowsPerWritePartition;
    }

    public void setBqSummaryRowsPerWritePartition(int bqSummaryRowsPerWritePartition) {
        this.bqSummaryRowsPerWritePartition = bqSummaryRowsPerWritePartition;
    }

    public int getBqDetailRowsPerWritePartition() {
        return bqDetailRowsPerWritePartition;
    }

    public void setBqDetailRowsPerWritePartition(int bqDetailRowsPerWritePartition) {
        this.bqDetailRowsPerWritePartition = bqDetailRowsPerWritePartition;
    }

    public int getBqAuditRowsPerWritePartition() {
        return bqAuditRowsPerWritePartition;
    }

    public void setBqAuditRowsPerWritePartition(int bqAuditRowsPerWritePartition) {
        this.bqAuditRowsPerWritePartition = bqAuditRowsPerWritePartition;
    }

    public long getBqMaxRowsPerLoadJob() {
        return bqMaxRowsPerLoadJob;
    }

    public void setBqMaxRowsPerLoadJob(long bqMaxRowsPerLoadJob) {
        this.bqMaxRowsPerLoadJob = bqMaxRowsPerLoadJob;
    }

    public int getMaxCachedDatabasesPerPartition() {
        return maxCachedDatabasesPerPartition;
    }

    public void setMaxCachedDatabasesPerPartition(int maxCachedDatabasesPerPartition) {
        this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition;
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

    public boolean isPartitionByFeatureSet() {
        return partitionByFeatureSet;
    }

    public void setPartitionByFeatureSet(boolean partitionByFeatureSet) {
        this.partitionByFeatureSet = partitionByFeatureSet;
    }

    public int getMaxTailChars() {
        return maxTailChars;
    }

    public void setMaxTailChars(int maxTailChars) {
        this.maxTailChars = maxTailChars;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public String toString() {
        return "ScanEngineProperties{maxCachedDatabasesPerPartition=" + maxCachedDatabasesPerPartition
                + ", sharedBundleCacheEnabled=" + sharedBundleCacheEnabled
                + ", maxSharedIdleBundlesPerExecutor=" + maxSharedIdleBundlesPerExecutor
                + ", sortPartitionsByFeatureSet=" + sortPartitionsByFeatureSet
                + ", partitionByFeatureSet=" + partitionByFeatureSet
                + ", maxTailChars=" + maxTailChars
                + ", createdBy='" + createdBy + "'"
                + ", autoSizeBundleCache=" + autoSizeBundleCache
                + ", bqSummaryRowsPerWritePartition=" + bqSummaryRowsPerWritePartition
                + ", bqDetailRowsPerWritePartition=" + bqDetailRowsPerWritePartition
                + ", bqAuditRowsPerWritePartition=" + bqAuditRowsPerWritePartition
                + ", bqMaxRowsPerLoadJob=" + bqMaxRowsPerLoadJob + "}";
    }
}
