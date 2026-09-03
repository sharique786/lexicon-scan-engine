package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.scanengine.util.LruCache;
import com.gliwka.hyperscan.wrapper.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Loads and caches, per feature, the {@link Database} and
 * {@link TermExpressionMetadata} the Lexicon Compile Service writes together
 * in a single {@code <feature>.zip} bundle on GCS — one instance per Spark
 * partition, lazy-loading and LRU-caching both together.
 *
 * <p>This class downloads and unzips the bundle EXACTLY ONCE per feature per
 * partition, since {@code FeatureScanOrchestrator} always needs both the
 * database and the metadata together for any feature it scans — caching them
 * as one {@link LexiconBundle} entry in one bounded {@link LruCache} avoids
 * downloading and unzipping the same file twice.
 *
 * <h2>Usage pattern</h2>
 * <p>Construct exactly ONE instance per Spark partition — inside a
 * {@code mapPartitions} closure, not a {@code map} closure — so the cache is
 * genuinely reused across every message in that partition. See
 * {@code PartitionProcessor}.
 *
 * <h2>Concurrent prefetch (JDK 21 virtual threads) — safe despite a non-thread-safe cache</h2>
 * <p>{@link #load}/{@link #loadDatabase}/{@link #loadMetadata} are NOT
 * thread-safe (backed by {@link LruCache}, which documents the same
 * constraint) — matching this class's own single-threaded, one-per-partition
 * usage contract. {@link #prefetch} still safely warms the cache
 * CONCURRENTLY for a batch of distinct features, via a two-phase design that
 * never lets more than one thread touch the cache itself:
 * <ol>
 *   <li>The expensive, cache-INDEPENDENT part — {@link #loadFresh} (GCS
 *       download, zip extraction, {@code Database.load},
 *       {@code TermExpressionMetadata.parse}) — runs concurrently, one
 *       virtual thread per feature, via {@link Executors#newVirtualThreadPerTaskExecutor()}.
 *       Virtual threads are the right tool here specifically because this
 *       work is I/O-BOUND (waiting on GCS network reads): many concurrent
 *       virtual threads blocked on I/O consume no executor CPU while
 *       waiting, so this does not compete for the Spark task's allocated
 *       CPU core(s) — a real consideration on a SHARED Dataproc cluster,
 *       where grabbing extra CPU beyond what YARN allocated this job would
 *       be poor multi-tenant behaviour. (CPU-bound Hyperscan scanning
 *       itself is deliberately NOT parallelised this way — see
 *       {@code FeatureScanOrchestrator} class Javadoc.)</li>
 *   <li>Once every concurrent load has completed (or failed) and every
 *       virtual thread has been joined, inserting the results into
 *       {@link #cache} happens SEQUENTIALLY, entirely on the calling
 *       thread — the only code path that ever touches the cache is this
 *       single-threaded loop, so {@link LruCache}'s own non-thread-safety
 *       is never actually exercised concurrently.</li>
 * </ol>
 */
public final class HyperscanBundleLoader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HyperscanBundleLoader.class);

    /**
     * Streams a GCS object's bytes — injected so this class's caching/
     * extraction/error-handling logic can be tested independently of a live
     * GCS client. A real implementation is a thin wrapper over
     * {@code Storage.reader(BlobId)} exposed as a {@code Channels.newInputStream}.
     */
    @FunctionalInterface
    public interface GcsByteStreamer {
        InputStream openStream(String gcsPath) throws IOException;
    }

    /** One feature's loaded database AND term metadata, extracted from the same zip bundle. */
    public record LexiconBundle(Database database, TermExpressionMetadata metadata) {
    }

    private final Map<String, String> featureToZipPath;
    private final GcsByteStreamer streamer;
    private final LruCache<String, LexiconBundle> cache;

    /**
     * @param featureToZipPath   resolved feature → {@code .zip} GCS path — the small,
     *                            broadcast-from-driver map (see {@code HyperscanPathResolver#buildZipPath})
     * @param streamer             opens a byte stream for a GCS path
     * @param maxCachedBundles    bounds this partition's cumulative cached-bundle count — a single
     *                            bound now covers both the native database and its metadata together,
     *                            since they are always loaded/evicted as one unit
     */
    public HyperscanBundleLoader(Map<String, String> featureToZipPath, GcsByteStreamer streamer,
                                  int maxCachedBundles) {
        this.featureToZipPath = featureToZipPath;
        this.streamer = streamer;
        this.cache = new LruCache<>(maxCachedBundles);
    }

    /**
     * Returns the loaded {@link LexiconBundle} for {@code feature}, downloading and extracting
     * its zip bundle on first request within this partition.
     *
     * @throws HyperscanPathResolver.HyperscanFileNotFoundException if {@code feature} has no resolved path
     * @throws HyperscanFileLoadException if the GCS stream, zip extraction, {@link Database#load},
     *          or {@link TermExpressionMetadata#parse} call fails
     */
    public LexiconBundle load(String feature) {
        return cache.computeIfAbsent(feature, this::loadFresh);
    }

    /** Convenience for callers that only need the database — see {@link #load}. */
    public Database loadDatabase(String feature) {
        return load(feature).database();
    }

    /** Convenience for callers that only need the term metadata — see {@link #load}. */
    public TermExpressionMetadata loadMetadata(String feature) {
        return load(feature).metadata();
    }

    /**
     * Concurrently warms the cache for up to {@code cache.maxSize()} distinct
     * features (first-seen order, i.e. {@code features}' own iteration
     * order — callers should pass features in roughly the order they'll
     * actually be needed, e.g. first-encountered-row order within a
     * partition) — see class Javadoc "Concurrent prefetch" for the
     * concurrency design and why it's safe. Capped at the cache's own bound
     * so this never does concurrent work for entries that would just be
     * evicted before ever being consulted.
     *
     * <p>Best-effort only: a feature that fails to prefetch (bad path,
     * corrupt zip, transient GCS error) is silently skipped here and simply
     * loads synchronously — with its real exception surfacing normally —
     * the first time {@link #load} actually needs it. Prefetching must never
     * be why a partition fails that would otherwise have succeeded.
     *
     * @param features   candidate features to warm — duplicates and features already
     *                    cached are harmless (deduplicated / naturally re-verified here)
     */
    public void prefetch(Collection<String> features) {
        if (features == null || features.isEmpty()) {
            return;
        }
        List<String> distinct = features.stream().distinct().limit(cache.maxSize()).toList();
        if (distinct.isEmpty()) {
            return;
        }

        Map<String, LexiconBundle> loaded = new LinkedHashMap<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<String, Future<LexiconBundle>> futures = new LinkedHashMap<>();
            for (String feature : distinct) {
                futures.put(feature, executor.submit(() -> loadFresh(feature)));
            }
            for (Map.Entry<String, Future<LexiconBundle>> entry : futures.entrySet()) {
                try {
                    loaded.put(entry.getKey(), entry.getValue().get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Prefetch interrupted for feature '{}' — will load synchronously when actually needed",
                            entry.getKey());
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    log.debug("Prefetch failed for feature '{}' — will retry synchronously when actually needed: {}",
                            entry.getKey(), cause.getMessage());
                }
            }
        }

        // Sequential insertion, entirely on the calling thread — see class Javadoc for why this
        // is the only place this method ever touches the (deliberately non-thread-safe) cache.
        for (Map.Entry<String, LexiconBundle> entry : loaded.entrySet()) {
            LexiconBundle bundle = entry.getValue();
            cache.computeIfAbsent(entry.getKey(), unusedFeature -> bundle);
        }
        log.debug("Prefetched {}/{} distinct feature bundle(s) concurrently for this partition",
                loaded.size(), distinct.size());
    }

    private LexiconBundle loadFresh(String feature) {
        String path = resolveZipPath(feature);
        ZipEntryBytes zipEntryBytes = extractZipEntries(feature, path);
        return buildBundle(feature, path, zipEntryBytes);
    }

    private String resolveZipPath(String feature) {
        String path = featureToZipPath.get(feature);
        if (path == null) {
            throw new HyperscanPathResolver.HyperscanFileNotFoundException(
                    "No resolved zip bundle path for feature '" + feature + "' — it was not present in the "
                    + "driver-resolved feature-to-path map. This indicates the view returned a feature "
                    + "this job's path resolution never saw, which should not happen if both derive from "
                    + "the same view query result.");
        }
        return path;
    }

    /** The two entries a feature's zip bundle must contain — raw bytes, not yet parsed. */
    private record ZipEntryBytes(byte[] hdbBytes, String metadataJson) {
    }

    private ZipEntryBytes extractZipEntries(String feature, String path) {
        byte[] hdbBytes = null;
        String metadataJson = null;
        List<String> entryNames = new ArrayList<>();
        try (InputStream in = streamer.openStream(path);
             ZipInputStream zip = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                String entryName = baseName(entry.getName());
                entryNames.add(entryName);
                if (entryName.equals(TermIdBuilder.hdbFileName(feature))) {
                    hdbBytes = readAllBytes(zip);
                } else if (entryName.equals(TermIdBuilder.termMetadataFileName(feature))) {
                    metadataJson = new String(readAllBytes(zip), StandardCharsets.UTF_8);
                }
                zip.closeEntry();
            }
        } catch (IOException | RuntimeException e) {
            throw new HyperscanFileLoadException(
                    "Failed to read zip bundle for feature '" + feature + "' from " + path
                    + ": " + e.getMessage(), e);
        }

        if (hdbBytes == null) {
            throw new HyperscanFileLoadException(
                    "Zip bundle for feature '" + feature + "' at " + path + " has no entry named '"
                    + TermIdBuilder.hdbFileName(feature) + "' — entries found: " + entryNames);
        }
        if (metadataJson == null) {
            throw new HyperscanFileLoadException(
                    "Zip bundle for feature '" + feature + "' at " + path + " has no entry named '"
                    + TermIdBuilder.termMetadataFileName(feature) + "' — entries found: " + entryNames);
        }
        return new ZipEntryBytes(hdbBytes, metadataJson);
    }

    private LexiconBundle buildBundle(String feature, String path, ZipEntryBytes zipEntryBytes) {
        try {
            Database database;
            try (InputStream hdbIn = new ByteArrayInputStream(zipEntryBytes.hdbBytes())) {
                database = Database.load(hdbIn);
            }
            TermExpressionMetadata metadata = TermExpressionMetadata.parse(feature, zipEntryBytes.metadataJson());
            return new LexiconBundle(database, metadata);
        } catch (IOException | RuntimeException e) {
            throw new HyperscanFileLoadException(
                    "Failed to load Hyperscan database or term metadata for feature '" + feature
                    + "' extracted from " + path + ": " + e.getMessage(), e);
        }
    }

    /** Strips any directory prefix a zip entry name might carry, e.g. {@code "sub/dir/x.hdb"} -> {@code "x.hdb"}. */
    private static String baseName(String entryName) {
        int slash = Math.max(entryName.lastIndexOf('/'), entryName.lastIndexOf('\\'));
        return slash < 0 ? entryName : entryName.substring(slash + 1);
    }

    private static byte[] readAllBytes(ZipInputStream zip) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        zip.transferTo(out);
        return out.toByteArray();
    }

    /** @return how many bundles are currently cached in this partition's loader. */
    public int cachedCount() {
        return cache.size();
    }

    @Override
    public void close() {
        // Database itself is Closeable (a native resource handle) — but this loader does not
        // proactively close cached entries on eviction, since a Database evicted from the cache
        // may still be in use by an in-flight Scanner in a concurrent context; relying on GC +
        // the native library's own finalisation is the safer default here.
    }
}
