package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.util.LruCache;
import com.gliwka.hyperscan.wrapper.Database;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Loads Hyperscan {@code .hdb} databases from GCS on demand, one instance
 * per Spark partition, caching loaded {@link Database} objects so repeated
 * messages within the same partition needing the same feature do not
 * re-download/re-parse it.
 *
 * <h2>Why streaming + per-partition caching, not driver-side broadcast of bytes</h2>
 * <p>An earlier implementation read every {@code .hdb} file's full bytes on
 * the DRIVER into a {@code Map<String, byte[]>} and broadcast that whole map
 * to every executor. With potentially many multi-megabyte files, this risks
 * driver OOM (holding every file in memory at once, before broadcasting)
 * and wastes executor memory (every executor holds the FULL map, including
 * features none of its own partition's messages ever need).
 *
 * <p>This class instead broadcasts only a small {@code Map<String, String>}
 * of feature name → resolved GCS path (see {@code HyperscanPathResolver} —
 * cheap to construct and broadcast, since it is just strings) and loads
 * actual file BYTES lazily, on an executor, only for a feature some message
 * in that executor's partition genuinely needs — via {@link GcsByteStreamer},
 * which streams rather than fully buffering a file before handing it to
 * {@link Database#load}. A bounded {@link LruCache} (see that class Javadoc)
 * keeps a long-lived executor's cumulative memory use predictable even if it
 * processes messages spanning many distinct features over its lifetime,
 * evicting the least-recently-used database rather than growing without
 * bound.
 *
 * <h2>Usage pattern</h2>
 * <p>Construct exactly ONE instance per Spark partition — inside a
 * {@code mapPartitions} closure, not a {@code map} closure — so the cache is
 * genuinely reused across every message in that partition rather than
 * rebuilt per message (see {@code FeatureScanOrchestrator}).
 */
public final class HyperscanDatabaseLoader implements AutoCloseable {

    /**
     * Streams a GCS object's bytes — injected so this class's caching/error-
     * handling logic can be tested independently of a live GCS client. A
     * real implementation is a thin wrapper over
     * {@code Storage.reader(BlobId)} exposed as a {@code Channels.newInputStream}.
     */
    @FunctionalInterface
    public interface GcsByteStreamer {
        InputStream openStream(String gcsPath) throws IOException;
    }

    private final Map<String, String> featureToPath;
    private final GcsByteStreamer streamer;
    private final LruCache<String, Database> cache;

    /**
     * @param featureToPath        resolved feature → {@code .hdb} GCS path — the small,
     *                              broadcast-from-driver map (see {@code HyperscanPathResolver})
     * @param streamer               opens a byte stream for a GCS path
     * @param maxCachedDatabases    bounds this partition's cumulative Hyperscan database memory —
     *                              see class Javadoc
     */
    public HyperscanDatabaseLoader(Map<String, String> featureToPath, GcsByteStreamer streamer,
                                    int maxCachedDatabases) {
        this.featureToPath = featureToPath;
        this.streamer = streamer;
        this.cache = new LruCache<>(maxCachedDatabases);
    }

    /**
     * Returns the loaded {@link Database} for {@code feature}, loading and
     * caching it on first request within this partition.
     *
     * @throws HyperscanPathResolver.HyperscanFileNotFoundException if {@code feature} has no
     *          resolved path (requirement 3.a)
     * @throws HyperscanFileLoadException if the GCS stream or {@link Database#load} call fails
     *          (requirement 3.b)
     */
    public Database load(String feature) {
        return cache.computeIfAbsent(feature, this::loadFresh);
    }

    private Database loadFresh(String feature) {
        String path = featureToPath.get(feature);
        if (path == null) {
            throw new HyperscanPathResolver.HyperscanFileNotFoundException(
                    "No resolved .hdb path for feature '" + feature + "' — it was not present in the "
                    + "driver-resolved feature-to-path map. This indicates the view returned a feature "
                    + "this job's path resolution never saw, which should not happen if both derive from "
                    + "the same view query result.");
        }
        try (InputStream in = streamer.openStream(path)) {
            return Database.load(in);
        } catch (IOException | RuntimeException e) {
            throw new HyperscanFileLoadException(
                    "Failed to load Hyperscan database for feature '" + feature + "' from " + path
                    + ": " + e.getMessage(), e);
        }
    }

    /** @return how many databases are currently cached in this partition's loader. */
    public int cachedCount() {
        return cache.size();
    }

    @Override
    public void close() {
        // Database itself is Closeable (a native resource handle) — but this loader does not
        // proactively close cached entries on eviction, since a Database evicted from the cache
        // may still be in use by an in-flight Scanner in a concurrent context; relying on GC +
        // the native library's own finalisation is the safer default here. close() on the whole
        // loader (called once the partition's processing is fully done) is a no-op placeholder
        // for now, kept as an explicit extension point rather than silently absent.
    }
}
