package com.db.macs3.ecomms.spectre.hyperscan;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.PreparedText;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoaderTest.simpleMetadataJson;
import static com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoaderTest.zipOf;

/**
 * Native-crash reproduction harness for the shared bundle cache (run with {@code -Dstress=true}). A crash shows up
 * as the forked test JVM dying (SIGSEGV), not as an assertion failure.
 */
@EnabledIfSystemProperty(named = "stress", matches = "true")
class SharedDatabaseConcurrencyStressTest {

    private static final int THREADS = 8;

    static byte[] richDbBytes() {
        EnumSet<ExpressionFlag> som = EnumSet.of(ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS, ExpressionFlag.UTF8);
        List<Expression> expressions = new ArrayList<>();
        String[] words = {"launder", "price", "spread", "keep", "mouth shut", "off shore", "account", "confidential",
                "manipulat\\w*", "guarantee", "insider", "rumou?r", "\\bbuy\\b", "\\bsell\\b", "kill"};
        int id = 1;
        for (String w : words) {
            expressions.add(new Expression(w, som, id++));
        }
        expressions.add(new Expression("This email may contain confidential.*", som, id++));
        expressions.add(new Expression("disclaimer.*", som, id++));
        expressions.add(new Expression("keep(?:\\s+\\S+){0,3}\\s+mouth shut", som, id++));
        try (Database database = Database.compile(expressions)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            database.save(out);
            return out.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String randomText() {
        String[] pool = {"launder", "price", "spread", "keep in the market mouth shut", "Off shore account",
                "This email may contain confidential information", "disclaimer applies", "股票", "שוק", "hello",
                "world", "insider rumour", "buy", "sell", "I'll kill you", "manipulate", "guarantee"};
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int n = 50 + r.nextInt(3000);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(pool[r.nextInt(pool.length)]).append(r.nextInt(10) == 0 ? '\n' : ' ');
        }
        return sb.toString();
    }

    /** Scenario A: several threads scan ONE native database concurrently, each with its own scanner. */
    @Test
    void manyThreadsScanOneSharedDatabase() throws Exception {
        byte[] bytes = richDbBytes();
        Database shared = com.gliwka.hyperscan.wrapper.TrailingDotStarRewriter.apply(
                Database.load(new ByteArrayInputStream(bytes)), bytes);
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    long matches = 0;
                    try (CollapsingScanner scanner = new CollapsingScanner()) {
                        for (int i = 0; i < 20_000; i++) {
                            scanner.allocScratch(shared);
                            try (PreparedText text = new PreparedText(randomText())) {
                                matches += scanner.scan(shared, text).size();
                            }
                        }
                    }
                    return matches;
                }));
            }
            for (Future<Long> f : futures) {
                System.out.println("scenario A thread matches: " + f.get());
            }
        }
        shared.close();
    }

    /** Scenario E: do scanners created at the same instant ever get the SAME native callback slot? */
    @Test
    void simultaneousScannerConstructionCallbackSlots() throws Exception {
        java.lang.reflect.Field handlerField = CollapsingScanner.class.getDeclaredField("handler");
        handlerField.setAccessible(true);
        int collisions = 0;
        for (int round = 0; round < 2000; round++) {
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(THREADS);
            List<CollapsingScanner> scanners = java.util.Collections.synchronizedList(new ArrayList<>());
            try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
                for (int t = 0; t < THREADS; t++) {
                    pool.submit(() -> {
                        barrier.await();
                        scanners.add(new CollapsingScanner());
                        return null;
                    });
                }
            }
            java.util.Set<Long> addresses = new java.util.HashSet<>();
            for (CollapsingScanner s : scanners) {
                long address = ((org.bytedeco.javacpp.Pointer) handlerField.get(s)).address();
                if (address == 0 || !addresses.add(address)) {
                    collisions++;
                }
            }
            scanners.forEach(CollapsingScanner::close);
        }
        System.out.println("scenario E callback slot collisions/NULL in 2000 rounds: " + collisions);
    }

    /** Scenario C: ONE never-closed shared database; only the scanners are created/closed concurrently. */
    @Test
    void scannerChurnOnly() throws Exception {
        byte[] bytes = richDbBytes();
        Database shared = Database.load(new ByteArrayInputStream(bytes));
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    long matches = 0;
                    for (int task = 0; task < 2000; task++) {
                        try (CollapsingScanner scanner = new CollapsingScanner()) {
                            for (int m = 0; m < 20; m++) {
                                scanner.allocScratch(shared);
                                try (PreparedText text = new PreparedText(randomText())) {
                                    matches += scanner.scan(shared, text).size();
                                }
                            }
                        }
                    }
                    return matches;
                }));
            }
            for (Future<Long> f : futures) {
                System.out.println("scenario C thread matches: " + f.get());
            }
        }
        shared.close();
    }

    /**
     * Scenario D: scanner churn over 4 databases. {@code -Dstress.private=true} gives each thread its own 4 copies;
     * otherwise all threads share the same 4. {@code -Dstress.rewrite=false} skips the trailing-.* recompile.
     */
    @Test
    void scannerChurnManyDatabases() throws Exception {
        byte[] bytes = richDbBytes();
        boolean privateDbs = Boolean.getBoolean("stress.private");
        boolean rewrite = !"false".equals(System.getProperty("stress.rewrite"));
        java.util.function.Supplier<List<Database>> fourDbs = () -> {
            List<Database> dbs = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                try {
                    Database db = Database.load(new ByteArrayInputStream(bytes));
                    dbs.add(rewrite ? com.gliwka.hyperscan.wrapper.TrailingDotStarRewriter.apply(db, bytes) : db);
                } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            return dbs;
        };
        List<Database> sharedDbs = privateDbs ? null : fourDbs.get();
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    List<Database> dbs = privateDbs ? fourDbs.get() : sharedDbs;
                    long matches = 0;
                    for (int task = 0; task < Integer.getInteger("stress.tasks", 200); task++) {
                        try (CollapsingScanner scanner = new CollapsingScanner()) {
                            for (int m = 0; m < 50; m++) {
                                try (PreparedText text = new PreparedText(randomText())) {
                                    for (Database db : dbs) {
                                        scanner.allocScratch(db);
                                        matches += scanner.scan(db, text).size();
                                    }
                                }
                            }
                        }
                    }
                    return matches;
                }));
            }
            for (Future<Long> f : futures) {
                System.out.println("scenario D thread matches: " + f.get());
            }
        }
    }

    /** Scenario B: the real loader + shared cache, with eviction churn (LRU 1, idle cap 0) while others scan. */
    @Test
    void sharedCacheChurnWhileScanning() throws Exception {
        boolean scannerPerTask = !Boolean.getBoolean("stress.longLivedScanner");
        byte[] bytes = richDbBytes();
        List<String> features = List.of("lexicon_s-a", "lexicon_s-b", "lexicon_s-c", "lexicon_s-d");
        Map<String, String> paths = Map.of(
                features.get(0), "gs://b/" + features.get(0) + ".zip",
                features.get(1), "gs://b/" + features.get(1) + ".zip",
                features.get(2), "gs://b/" + features.get(2) + ".zip",
                features.get(3), "gs://b/" + features.get(3) + ".zip");
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            String feature = features.stream().filter(path::contains).findFirst().orElseThrow();
            return new ByteArrayInputStream(zipOf(feature, bytes, simpleMetadataJson(feature)));
        };
        java.util.Map<Database, java.util.concurrent.atomic.AtomicInteger> inUse =
                java.util.Collections.synchronizedMap(new java.util.IdentityHashMap<>());
        java.util.concurrent.atomic.AtomicInteger violations = new java.util.concurrent.atomic.AtomicInteger();
        CollapsingScannerPool scannerPool = Boolean.getBoolean("stress.pool") ? new CollapsingScannerPool(THREADS) : null;
        SharedBundleCache shared = new SharedBundleCache(Integer.getInteger("stress.idle", 0), bundle -> {
            java.util.concurrent.atomic.AtomicInteger users = inUse.get(bundle.database());
            if (users != null && users.get() > 0) {
                if (violations.incrementAndGet() <= 3) {
                    new IllegalStateException("closing a database " + users.get() + " thread(s) are scanning, on "
                            + Thread.currentThread()).printStackTrace();
                }
                return; // don't close it: that would crash the JVM
            }
            HyperscanBundleLoader.closeBundleQuietly(bundle);
        });
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    long matches = 0;
                    CollapsingScanner longLived = scannerPerTask ? null : new CollapsingScanner();
                    for (int task = 0; task < Integer.getInteger("stress.tasks", 30); task++) { // one "Spark task" per iteration
                        try (HyperscanBundleLoader loader = new HyperscanBundleLoader(paths, streamer,
                                Integer.getInteger("stress.lru", 1), Boolean.getBoolean("stress.private") ? null : shared, "run");
                             AutoCloseable perTask = !scannerPerTask ? null : scannerPool != null
                                     ? pooled(scannerPool) : new CollapsingScanner()) {
                            CollapsingScanner scanner = !scannerPerTask ? longLived
                                    : perTask instanceof PooledScanner p ? p.scanner() : (CollapsingScanner) perTask;
                            if (!Boolean.getBoolean("stress.noPrefetch")) { loader.prefetch(features); }
                            for (int m = 0; m < 50; m++) {
                                String text = randomText();
                                try (PreparedText prepared = new PreparedText(text)) {
                                    for (String feature : features) {
                                        Database db = loader.loadDatabase(feature);
                                        java.util.concurrent.atomic.AtomicInteger users = inUse.computeIfAbsent(db,
                                                k -> new java.util.concurrent.atomic.AtomicInteger());
                                        users.incrementAndGet();
                                        try {
                                            scanner.allocScratch(db);
                                            matches += scanner.scan(db, prepared).size();
                                        } finally {
                                            users.decrementAndGet();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (longLived != null) {
                        longLived.close();
                    }
                    return matches;
                }));
            }
            for (Future<Long> f : futures) {
                System.out.println("scenario B thread matches: " + f.get());
            }
        }
        System.out.println("scenario B close-while-scanning violations: " + violations.get());
        shared.closeIdle();
    }

    /** A scanner borrowed for one "task" and given back to the pool on close, as production does. */
    private record PooledScanner(CollapsingScannerPool pool, CollapsingScanner scanner) implements AutoCloseable {
        @Override
        public void close() {
            pool.release(scanner);
        }
    }

    private static PooledScanner pooled(CollapsingScannerPool pool) {
        return new PooledScanner(pool, pool.borrow(CollapsingScanner.DEFAULT_MAX_TAIL_CHARS));
    }
}
