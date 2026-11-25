package com.db.macs3.ecomms.spectre.spark;

/**
 * Effective Hyperscan bundle-cache sizes for one run, derived from the largest number of lexicon features any
 * single message needs.
 *
 * <h2>Why</h2>
 * Every message scans its features in the same order, so the per-partition {@code LruCache} sees a CYCLIC access
 * pattern. LRU on a cycle longer than its capacity has a 0% hit rate: with 164 features per message and a cache of
 * 20 (or 50), every one of the 164 bundles is evicted before it is needed again, so EVERY message re-downloads,
 * unzips, {@code Database.load}s and (for {@code .*} terms) recompiles all 164 bundles — 40M native loads for 248k
 * messages. That native churn is what killed the executors on the 41M-view-row run; the 9.8M-row run (40-50
 * features per message) survived only because 50 >= its working set.
 *
 * <p>So the per-partition cache must hold at least one message's whole feature set, and the executor-wide idle
 * cache must too, or a new task reloads what the previous task just released. With the shared cache on, raising
 * the per-partition cap costs no extra native memory (its entries are leases on the ONE executor copy).
 */
public record BundleCacheSizing(int perPartition, int sharedIdle) {

    /**
     * @param maxFeaturesPerMessage largest number of view rows (features) of any one message in this run
     * @param sharedEnabled         {@code scan-engine.shared-bundle-cache-enabled}
     * @param autoSize              {@code scan-engine.auto-size-bundle-cache}; false keeps the configured values
     * @return {@code sharedIdle} is negative when the shared cache is disabled (PartitionProcessor's convention)
     */
    public static BundleCacheSizing resolve(int configuredPerPartition, int configuredSharedIdle,
                                            boolean sharedEnabled, boolean autoSize, long maxFeaturesPerMessage) {
        int workingSet = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, maxFeaturesPerMessage));
        int perPartition = autoSize ? Math.max(configuredPerPartition, workingSet) : configuredPerPartition;
        if (!sharedEnabled) {
            return new BundleCacheSizing(perPartition, -1);
        }
        int sharedIdle = autoSize ? Math.max(configuredSharedIdle, workingSet) : configuredSharedIdle;
        return new BundleCacheSizing(perPartition, sharedIdle);
    }

    /** True when a message needs more bundles than the per-partition cache holds — every message reloads them. */
    public static boolean thrashes(int perPartition, long maxFeaturesPerMessage) {
        return maxFeaturesPerMessage > perPartition;
    }
}
