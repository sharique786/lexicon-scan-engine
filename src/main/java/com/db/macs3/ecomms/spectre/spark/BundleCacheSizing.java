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
 * cache must too, or a new task reloads what the previous task just released.
 *
 * <h2>Shared cache on: size both to the run's DISTINCT bundle count</h2>
 * Different messages need different feature sets, so an executor's tasks together touch (almost) every distinct
 * bundle of the run. An idle cap below that count closes bundles that the next task reloads (download, native load,
 * recompile) — the reload churn seen on the 20M-record run (163 distinct bundles, ~110 per message, idle cap ~110).
 * The shared cache never holds more than one copy per distinct bundle, so sizing it to the distinct count costs NO
 * extra peak native memory: each executor loads every bundle at most once and keeps it for the run. The
 * per-partition cache holds only leases on those shared copies, so it is sized the same way. Native memory per
 * executor is then (sum of the run's .hdb sizes) — size {@code spark.executor.memoryOverhead} for it.
 *
 * <p>Shared cache off: each task holds PRIVATE copies, so the per-partition cache is sized to one message's feature
 * set only (sizing it to the distinct count would multiply native memory by the task count).
 */
public record BundleCacheSizing(int perPartition, int sharedIdle) {

    /**
     * @param maxFeaturesPerMessage largest number of view rows (features) of any one message in this run
     * @param distinctFeatures      number of distinct features (bundles) in this run
     * @param sharedEnabled         {@code scan-engine.shared-bundle-cache-enabled}
     * @param autoSize              {@code scan-engine.auto-size-bundle-cache}; false keeps the configured values
     * @return {@code sharedIdle} is negative when the shared cache is disabled (PartitionProcessor's convention)
     */
    public static BundleCacheSizing resolve(int configuredPerPartition, int configuredSharedIdle,
                                            boolean sharedEnabled, boolean autoSize, long maxFeaturesPerMessage,
                                            long distinctFeatures) {
        int perMessage = clamp(maxFeaturesPerMessage);
        if (!sharedEnabled) {
            return new BundleCacheSizing(autoSize ? Math.max(configuredPerPartition, perMessage)
                    : configuredPerPartition, -1);
        }
        if (!autoSize) {
            return new BundleCacheSizing(configuredPerPartition, configuredSharedIdle);
        }
        int all = Math.max(perMessage, clamp(distinctFeatures));
        return new BundleCacheSizing(Math.max(configuredPerPartition, all), Math.max(configuredSharedIdle, all));
    }

    private static int clamp(long value) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, value));
    }

    /** True when a message needs more bundles than the per-partition cache holds — every message reloads them. */
    public static boolean thrashes(int perPartition, long maxFeaturesPerMessage) {
        return maxFeaturesPerMessage > perPartition;
    }

    /** True when the shared idle cache cannot keep every distinct bundle — tasks reload what earlier tasks released. */
    public static boolean churns(int sharedIdle, long distinctFeatures) {
        return sharedIdle >= 0 && distinctFeatures > sharedIdle;
    }
}
