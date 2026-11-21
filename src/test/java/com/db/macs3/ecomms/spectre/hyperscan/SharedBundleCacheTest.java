package com.db.macs3.ecomms.spectre.hyperscan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SharedBundleCache")
class SharedBundleCacheTest {

    /** A stand-in bundle — the cache never looks inside it; only the closer does. */
    private static HyperscanBundleLoader.LexiconBundle bundle() {
        return new HyperscanBundleLoader.LexiconBundle(null, null);
    }

    private final List<HyperscanBundleLoader.LexiconBundle> closed = new CopyOnWriteArrayList<>();

    private SharedBundleCache cache(int maxIdle) {
        return new SharedBundleCache(maxIdle, closed::add);
    }

    @Test
    @DisplayName("a second acquire of the same key reuses the loaded bundle without loading again")
    void reusesLoadedBundle() {
        SharedBundleCache cache = cache(4);
        AtomicInteger loads = new AtomicInteger();
        HyperscanBundleLoader.LexiconBundle loaded = bundle();

        SharedBundleCache.Lease first = cache.acquire("k", () -> {
            loads.incrementAndGet();
            return loaded;
        });
        SharedBundleCache.Lease second = cache.acquire("k", () -> {
            loads.incrementAndGet();
            return bundle();
        });

        assertThat(loads).hasValue(1);
        assertThat(second.bundle()).isSameAs(first.bundle()).isSameAs(loaded);
        assertThat(cache.openCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a released bundle stays open as idle and is reused by the next acquire")
    void releasedBundleStaysIdleAndIsReused() {
        SharedBundleCache cache = cache(4);
        AtomicInteger loads = new AtomicInteger();

        SharedBundleCache.Lease lease = cache.acquire("k", () -> {
            loads.incrementAndGet();
            return bundle();
        });
        lease.close();

        assertThat(cache.idleCount()).isEqualTo(1);
        assertThat(closed).isEmpty();

        cache.acquire("k", () -> {
            loads.incrementAndGet();
            return bundle();
        });
        assertThat(loads).hasValue(1);
        assertThat(cache.idleCount()).as("leased again, so no longer idle").isZero();
    }

    @Test
    @DisplayName("only the least-recently released IDLE bundle is closed when the idle cap is exceeded")
    void evictsLeastRecentlyUsedIdleBundle() {
        SharedBundleCache cache = cache(2);
        HyperscanBundleLoader.LexiconBundle a = bundle();
        HyperscanBundleLoader.LexiconBundle b = bundle();
        HyperscanBundleLoader.LexiconBundle c = bundle();

        cache.acquire("a", () -> a).close();
        cache.acquire("b", () -> b).close();
        cache.acquire("c", () -> c).close();

        assertThat(closed).hasSize(1);
        assertThat(closed.getFirst()).isSameAs(a);
        assertThat(cache.idleCount()).isEqualTo(2);
        assertThat(cache.openCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("a leased bundle is never closed, however far over the idle cap the cache goes")
    void neverClosesLeasedBundle() {
        SharedBundleCache cache = cache(0);
        HyperscanBundleLoader.LexiconBundle held = bundle();
        SharedBundleCache.Lease lease = cache.acquire("held", () -> held);

        cache.acquire("other", SharedBundleCacheTest::bundle).close(); // idle cap 0: closed at once
        cache.closeIdle();

        assertThat(closed).hasSize(1);
        assertThat(closed.getFirst()).isNotSameAs(held);
        lease.close();
        assertThat(closed).hasSize(2);
        assertThat(closed.get(1)).isSameAs(held);
    }

    @Test
    @DisplayName("the bundle is closed only after the LAST of several leases is released")
    void closesOnlyAfterLastLease() {
        SharedBundleCache cache = cache(0);
        HyperscanBundleLoader.LexiconBundle shared = bundle();
        SharedBundleCache.Lease first = cache.acquire("k", () -> shared);
        SharedBundleCache.Lease second = cache.acquire("k", SharedBundleCacheTest::bundle);

        first.close();
        assertThat(closed).isEmpty();
        second.close();
        assertThat(closed).hasSize(1);
        assertThat(closed.getFirst()).isSameAs(shared);
    }

    @Test
    @DisplayName("closing a lease twice releases it only once")
    void leaseCloseIsIdempotent() {
        SharedBundleCache cache = cache(0);
        SharedBundleCache.Lease first = cache.acquire("k", SharedBundleCacheTest::bundle);
        SharedBundleCache.Lease second = cache.acquire("k", SharedBundleCacheTest::bundle);

        first.close();
        first.close(); // must not count as releasing `second`'s reference too
        assertThat(closed).isEmpty();

        second.close();
        assertThat(closed).hasSize(1);
    }

    @Test
    @DisplayName("a failed load is not cached: the caller gets the original exception and the next acquire retries")
    void failedLoadIsNotCached() {
        SharedBundleCache cache = cache(4);
        IllegalStateException failure = new IllegalStateException("GCS down");

        assertThatThrownBy(() -> cache.acquire("k", () -> {
            throw failure;
        })).isSameAs(failure);
        assertThat(cache.openCount()).isZero();

        HyperscanBundleLoader.LexiconBundle retried = bundle();
        assertThat(cache.acquire("k", () -> retried).bundle()).isSameAs(retried);
    }

    @Test
    @DisplayName("concurrent first requests for one key load it exactly once and all get the same bundle")
    void concurrentAcquiresLoadOnce() throws Exception {
        SharedBundleCache cache = cache(4);
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        int threads = 8;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<HyperscanBundleLoader.LexiconBundle>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> cache.acquire("k", () -> {
                    loads.incrementAndGet();
                    loaderEntered.countDown();
                    try {
                        releaseLoader.await(10, TimeUnit.SECONDS); // hold the load so the others must wait on it
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return bundle();
                }).bundle()));
            }
            assertThat(loaderEntered.await(10, TimeUnit.SECONDS)).isTrue();
            releaseLoader.countDown();

            HyperscanBundleLoader.LexiconBundle first = results.getFirst().get(10, TimeUnit.SECONDS);
            for (Future<HyperscanBundleLoader.LexiconBundle> result : results) {
                assertThat(result.get(10, TimeUnit.SECONDS)).isSameAs(first);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(loads).hasValue(1);
    }

    @Test
    @DisplayName("a negative idle cap is rejected")
    void rejectsNegativeIdleCap() {
        assertThatThrownBy(() -> new SharedBundleCache(-1, b -> { }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("forExecutor returns one JVM-wide instance")
    void forExecutorIsSingleton() {
        assertThat(SharedBundleCache.forExecutor(3)).isSameAs(SharedBundleCache.forExecutor(99));
    }
}
