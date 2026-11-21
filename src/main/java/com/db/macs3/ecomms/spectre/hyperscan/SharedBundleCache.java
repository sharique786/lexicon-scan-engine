package com.db.macs3.ecomms.spectre.hyperscan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Executor-wide (one per JVM), reference-counted cache of loaded Hyperscan bundles, shared by every Spark task
 * running on that executor.
 *
 * <h2>Why</h2>
 * Without it, every task (partition) downloads, unzips, deserialises and — for {@code .*} terms — recompiles its
 * own copy of every bundle it needs. With ~400 bundles and thousands of tasks over a 10M-message run, the same bundle
 * is rebuilt thousands of times, and N concurrent tasks on one executor hold N native copies of it. A Hyperscan
 * database is read-only once built and can be scanned concurrently by any number of threads, each with its own
 * scratch (the scanner owns the scratch, not the database), so one copy per executor is enough.
 *
 * <h2>Lifecycle — never closes a database someone is using</h2>
 * <ul>
 *   <li>{@link #acquire} returns a {@link Lease}; the bundle stays open until EVERY lease on it is released.</li>
 *   <li>A bundle with no leases goes to an idle LRU list. Only idle bundles are ever closed, and only once more than
 *       {@code maxIdleBundles} of them are idle (or {@link #closeIdle()} is called).</li>
 *   <li>Concurrent first requests for the same key load it ONCE; the others wait for that load. A failed load is not
 *       cached: every waiter gets the failure, and the next request retries.</li>
 * </ul>
 * Native memory is therefore bounded by (bundles currently leased by running tasks) + {@code maxIdleBundles}.
 *
 * <p>Thread-safe. All bookkeeping is under {@code this}'s monitor; loading and closing happen outside it.
 */
public final class SharedBundleCache {

    private static final Logger log = LoggerFactory.getLogger(SharedBundleCache.class);

    private static final Object INSTANCE_LOCK = new Object();
    private static SharedBundleCache executorInstance; // guarded by INSTANCE_LOCK

    private final int maxIdleBundles;
    private final Consumer<HyperscanBundleLoader.LexiconBundle> closer;

    private final Map<String, Entry> entries = new HashMap<>();                   // guarded by this
    private final LinkedHashMap<String, Entry> idle = new LinkedHashMap<>(16, 0.75f, true); // guarded by this

    private static final class Entry {
        final CompletableFuture<HyperscanBundleLoader.LexiconBundle> bundle = new CompletableFuture<>();
        int leases;
    }

    /**
     * @param maxIdleBundles how many unleased bundles to keep open for reuse (must be &gt;= 0)
     * @param closer         frees one bundle's native resources; called exactly once per bundle, outside any lock
     */
    public SharedBundleCache(int maxIdleBundles, Consumer<HyperscanBundleLoader.LexiconBundle> closer) {
        if (maxIdleBundles < 0) {
            throw new IllegalArgumentException("maxIdleBundles must be >= 0, got " + maxIdleBundles);
        }
        this.maxIdleBundles = maxIdleBundles;
        this.closer = closer;
    }

    /**
     * The executor JVM's single instance, created on first use with {@code maxIdleBundles}; later calls return the
     * same instance (the first caller's capacity wins — every task of one job passes the same value).
     */
    public static SharedBundleCache forExecutor(int maxIdleBundles) {
        synchronized (INSTANCE_LOCK) {
            if (executorInstance == null) {
                executorInstance = new SharedBundleCache(maxIdleBundles, HyperscanBundleLoader::closeBundleQuietly);
                log.info("Created executor-wide Hyperscan bundle cache (max {} idle bundle(s))", maxIdleBundles);
            }
            return executorInstance;
        }
    }

    /** A held reference to a shared bundle. {@link #close()} releases it; idempotent. */
    public final class Lease implements AutoCloseable {
        private final String key;
        private final Entry entry;
        private final HyperscanBundleLoader.LexiconBundle bundle;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(String key, Entry entry, HyperscanBundleLoader.LexiconBundle bundle) {
            this.key = key;
            this.entry = entry;
            this.bundle = bundle;
        }

        public HyperscanBundleLoader.LexiconBundle bundle() {
            return bundle;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release(key, entry);
            }
        }
    }

    /**
     * Leases the bundle for {@code key}, loading it with {@code loader} if no copy is open. The caller MUST close the
     * returned lease when done with the bundle.
     *
     * @throws RuntimeException whatever {@code loader} threw (unwrapped), for this caller and every concurrent waiter
     */
    public Lease acquire(String key, Supplier<HyperscanBundleLoader.LexiconBundle> loader) {
        Entry entry;
        boolean loadHere = false;
        synchronized (this) {
            entry = entries.get(key);
            if (entry == null) {
                entry = new Entry();
                entries.put(key, entry);
                loadHere = true;
            }
            entry.leases++;
            idle.remove(key);
        }

        if (loadHere) {
            load(key, entry, loader);
        }
        try {
            return new Lease(key, entry, entry.bundle.join());
        } catch (CompletionException e) {
            synchronized (this) {
                entry.leases--; // the failed entry was already unmapped by the loading thread
            }
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(cause);
        }
    }

    private void load(String key, Entry entry, Supplier<HyperscanBundleLoader.LexiconBundle> loader) {
        try {
            entry.bundle.complete(loader.get());
        } catch (RuntimeException | Error e) {
            synchronized (this) {
                if (entries.get(key) == entry) {
                    entries.remove(key); // never cache a failure — the next acquire retries
                }
            }
            entry.bundle.completeExceptionally(e);
        }
    }

    private void release(String key, Entry entry) {
        List<HyperscanBundleLoader.LexiconBundle> toClose = new ArrayList<>();
        synchronized (this) {
            entry.leases--;
            if (entry.leases > 0 || entries.get(key) != entry) {
                return;
            }
            idle.put(key, entry);
            Iterator<Map.Entry<String, Entry>> eldest = idle.entrySet().iterator();
            while (idle.size() > maxIdleBundles && eldest.hasNext()) {
                Map.Entry<String, Entry> victim = eldest.next();
                eldest.remove();
                entries.remove(victim.getKey());
                toClose.add(victim.getValue().bundle.join()); // idle => loaded successfully
            }
        }
        toClose.forEach(closer);
    }

    /** Closes every idle (unleased) bundle now. Leased bundles are untouched. */
    public void closeIdle() {
        List<HyperscanBundleLoader.LexiconBundle> toClose = new ArrayList<>();
        synchronized (this) {
            for (Map.Entry<String, Entry> e : idle.entrySet()) {
                entries.remove(e.getKey());
                toClose.add(e.getValue().bundle.join());
            }
            idle.clear();
        }
        toClose.forEach(closer);
    }

    /** Number of open bundles (leased or idle). */
    public synchronized int openCount() {
        int count = 0;
        for (Entry e : entries.values()) {
            if (e.bundle.isDone() && !e.bundle.isCompletedExceptionally()) {
                count++;
            }
        }
        return count;
    }

    /** Number of open bundles nobody currently leases. */
    public synchronized int idleCount() {
        return idle.size();
    }

    public int maxIdleBundles() {
        return maxIdleBundles;
    }
}
