package com.db.macs3.ecomms.spectre.scanengine.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("LruCache")
class LruCacheTest {

    @Test
    @DisplayName("computeIfAbsent loads and caches distinct keys")
    void loadsDistinctKeys() {
        List<String> loadCalls = new ArrayList<>();
        LruCache<String, String> cache = new LruCache<>(3);

        assertThat(cache.computeIfAbsent("A", k -> { loadCalls.add(k); return "valA"; })).isEqualTo("valA");
        assertThat(cache.computeIfAbsent("B", k -> { loadCalls.add(k); return "valB"; })).isEqualTo("valB");
        assertThat(loadCalls).hasSize(2);
    }

    @Test
    @DisplayName("a cache hit does not re-invoke the loader")
    void cacheHitSkipsLoader() {
        List<String> loadCalls = new ArrayList<>();
        LruCache<String, String> cache = new LruCache<>(3);
        cache.computeIfAbsent("A", k -> { loadCalls.add(k); return "valA"; });
        cache.computeIfAbsent("A", k -> { loadCalls.add(k); return "valA-RELOADED"; });
        assertThat(loadCalls).hasSize(1);
    }

    @Test
    @DisplayName("eviction removes the LEAST recently used entry, not simply the oldest inserted")
    void evictsLeastRecentlyUsed() {
        LruCache<String, String> cache = new LruCache<>(3);
        cache.computeIfAbsent("A", k -> "valA");
        cache.computeIfAbsent("B", k -> "valB");
        cache.computeIfAbsent("C", k -> "valC");

        // Re-access A -- moves it to most-recently-used, leaving B as the LRU entry.
        cache.computeIfAbsent("A", k -> "valA-RELOADED");

        cache.computeIfAbsent("D", k -> "valD"); // triggers eviction

        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.containsKey("B")).isFalse(); // evicted
        assertThat(cache.containsKey("A")).isTrue();  // survived (recently accessed)
        assertThat(cache.containsKey("C")).isTrue();
        assertThat(cache.containsKey("D")).isTrue();
    }

    @Test
    @DisplayName("an evicted key triggers a genuine reload on next access")
    void evictedKeyReloads() {
        List<String> loadCalls = new ArrayList<>();
        LruCache<String, String> cache = new LruCache<>(2);
        cache.computeIfAbsent("A", k -> { loadCalls.add(k); return "valA"; });
        cache.computeIfAbsent("B", k -> { loadCalls.add(k); return "valB"; });
        cache.computeIfAbsent("C", k -> { loadCalls.add(k); return "valC"; }); // evicts A
        cache.computeIfAbsent("A", k -> { loadCalls.add(k); return "valA2"; }); // reload

        assertThat(loadCalls).containsExactly("A", "B", "C", "A");
    }

    @Test
    @DisplayName("maxSize() reflects the configured bound")
    void maxSizeReflectsBound() {
        assertThat(new LruCache<String, String>(7).maxSize()).isEqualTo(7);
    }

    @Test
    @DisplayName("a non-positive maxSize is rejected at construction")
    void rejectsNonPositiveMaxSize() {
        assertThatThrownBy(() -> new LruCache<String, String>(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LruCache<String, String>(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
