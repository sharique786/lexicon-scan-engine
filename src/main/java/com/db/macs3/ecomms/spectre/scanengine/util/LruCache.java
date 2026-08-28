package com.db.macs3.ecomms.spectre.scanengine.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A bounded, least-recently-used cache with on-demand loading — the
 * mechanism behind keeping executor-side Hyperscan {@code Database} memory
 * use predictable (see {@code HyperscanDatabaseLoader} class Javadoc for why
 * this exists: broadcasting whole {@code .hdb} file contents from the driver
 * risks driver OOM for large/many files, and loading every distinct feature
 * an executor ever touches with no eviction risks unbounded executor memory
 * growth over a long-lived job).
 *
 * <p>Backed by {@link LinkedHashMap}'s built-in access-order + eviction
 * support ({@code removeEldestEntry}) rather than a hand-rolled linked
 * structure — this is the standard, well-understood way to build a bounded
 * LRU cache in plain Java with no external dependency.
 *
 * <p><b>Not thread-safe.</b> Callers needing concurrent access (e.g. a
 * multi-threaded executor task) must synchronise externally, or use one
 * instance per thread — see {@code HyperscanDatabaseLoader}, which uses one
 * instance per Spark partition/task, matching Spark's own per-task
 * single-threaded execution model.
 *
 * @param <K> cache key
 * @param <V> cached value
 */
public final class LruCache<K, V> {

    private final int maxSize;
    private final LinkedHashMap<K, V> delegate;

    public LruCache(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive, got " + maxSize);
        }
        this.maxSize = maxSize;
        // accessOrder=true: get() moves an entry to "most recently used" position,
        // not just put() — required for genuine LRU (not just insertion-order FIFO) eviction.
        this.delegate = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > LruCache.this.maxSize;
            }
        };
    }

    /**
     * Returns the cached value for {@code key}, computing and caching it via
     * {@code loader} on a miss. If the cache is at capacity, the
     * least-recently-used entry is evicted first (see class Javadoc).
     *
     * @param loader    called at most once per distinct {@code key} between evictions —
     *                   NOT re-invoked on a cache hit
     */
    public V computeIfAbsent(K key, Function<K, V> loader) {
        V cached = delegate.get(key);
        if (cached != null) {
            return cached;
        }
        V loaded = loader.apply(key);
        delegate.put(key, loaded);
        return loaded;
    }

    public int size() {
        return delegate.size();
    }

    public boolean containsKey(K key) {
        return delegate.containsKey(key);
    }

    public int maxSize() {
        return maxSize;
    }
}
