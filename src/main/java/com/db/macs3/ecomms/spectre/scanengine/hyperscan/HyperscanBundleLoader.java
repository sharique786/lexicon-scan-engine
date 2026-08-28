package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.scanengine.util.LruCache;
import com.gliwka.hyperscan.wrapper.Database;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Loads and caches, per feature, the {@link Database} AND
 * {@link TermExpressionMetadata} the Lexicon Compile Service now writes
 * TOGETHER in a single {@code <feature>.zip} bundle on GCS — one instance per
 * Spark partition, mirroring {@code HyperscanDatabaseLoader}/
 * {@code TermMetadataLoader}'s original per-partition, lazy-load, LRU-cached
 * pattern, now unified into one loader/one cache.
 *
 * <h2>Replaces two separate loaders — because the underlying resource is now one file</h2>
 * <p>Before this change, the Compile Service wrote two separate files per
 * feature ({@code <feature>.hdb}, {@code <feature>-compile-results.json}),
 * and this project accordingly used two independent loaders/broadcasts/caches
 * ({@code HyperscanDatabaseLoader}, {@code TermMetadataLoader}) — a
 * deliberate design at the time, reasoned from the fact that a
 * {@link Database} (heavy, native/off-heap) and a {@link TermExpressionMetadata}
 * (light, on-heap plain id lists) had genuinely different memory-bounding
 * needs and came from genuinely independent GCS objects.
 *
 * <p>The Compile Service now writes ONE {@code <feature>.zip} containing both
 * as entries. Keeping two independent loaders under this new scheme would
 * mean downloading and unzipping the SAME file twice per feature per
 * partition — once for the database, once for the metadata — for no benefit,
 * since {@code FeatureScanOrchestrator} always needs both together for any
 * feature it scans anyway. This class downloads and unzips the bundle
 * EXACTLY ONCE per feature per partition, caching both resulting objects
 * together as one {@link LexiconBundle} entry in one bounded {@link LruCache}.
 *
 * <h2>Usage pattern</h2>
 * <p>Construct exactly ONE instance per Spark partition — inside a
 * {@code mapPartitions} closure, not a {@code map} closure — so the cache is
 * genuinely reused across every message in that partition. See
 * {@code PartitionProcessor}.
 */
public final class HyperscanBundleLoader implements AutoCloseable {

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

    private LexiconBundle loadFresh(String feature) {
        String path = featureToZipPath.get(feature);
        if (path == null) {
            throw new HyperscanPathResolver.HyperscanFileNotFoundException(
                    "No resolved zip bundle path for feature '" + feature + "' — it was not present in the "
                    + "driver-resolved feature-to-path map. This indicates the view returned a feature "
                    + "this job's path resolution never saw, which should not happen if both derive from "
                    + "the same view query result.");
        }

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

        try {
            Database database;
            try (InputStream hdbIn = new ByteArrayInputStream(hdbBytes)) {
                database = Database.load(hdbIn);
            }
            TermExpressionMetadata metadata = TermExpressionMetadata.parse(feature, metadataJson);
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
        // the native library's own finalisation is the safer default here, mirroring the
        // superseded HyperscanDatabaseLoader's identical reasoning.
    }
}
