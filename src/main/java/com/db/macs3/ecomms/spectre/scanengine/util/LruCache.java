package com.db.macs3.ecomms.spectre.scanengine.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A bounded, least-recently-used cache with on-demand loading — used by
 * {@code HyperscanBundleLoader} to keep executor-side Hyperscan bundle
 * memory use predictable, since loading every distinct feature an executor
 * ever touches with no eviction would risk unbounded memory growth over a
 * long-lived job.
 *
 * <p>Backed by {@link LinkedHashMap}'s built-in access-order + eviction
 * support ({@code removeEldestEntry}) rather than a hand-rolled linked
 * structure.
 *
 * <p><b>Not thread-safe.</b> Callers needing concurrent access must
 * synchronise externally, or use one instance per thread — see
 * {@code HyperscanBundleLoader}, which uses one instance per Spark
 * partition/task, matching Spark's own per-task single-threaded execution
 * model.
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
        V cachedValue = delegate.get(key);
        if (cachedValue != null) {
            return cachedValue;
        }
        V loadedValue = loader.apply(key);
        delegate.put(key, loadedValue);
        return loadedValue;
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
