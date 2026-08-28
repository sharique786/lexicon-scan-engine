package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.scanengine.util.LruCache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Loads and caches one feature's {@link TermExpressionMetadata} from GCS —
 * the sibling of {@link HyperscanDatabaseLoader}, following the exact same
 * per-partition, lazy-load, LRU-cached pattern for the SAME reason: cheap to
 * broadcast a small {@code Map<String, String>} of resolved paths from the
 * driver, expensive to eagerly read every feature's metadata file up front,
 * unnecessary to hold metadata for features this partition's messages never
 * touch.
 *
 * <h2>Why this now exists — confirmed gap</h2>
 * <p>See {@link TermExpressionMetadata} class Javadoc for the full
 * explanation: the Compile Service's AND NOT fix means a {@code .hdb} file
 * alone is no longer self-sufficient for resolving an AND NOT term's matched
 * expression id back to a correct {@code term_id}, or for evaluating its
 * boolean condition at all. This loader reads the accompanying
 * {@code <feature>-compile-results.json} the Compile Service writes
 * alongside every {@code /compile/bundle} database (see
 * {@code HyperscanPathResolver#buildTermMetadataPath}) to supply that
 * missing information.
 *
 * <h2>Usage pattern</h2>
 * <p>Construct exactly ONE instance per Spark partition, alongside its
 * {@link HyperscanDatabaseLoader} — inside a {@code mapPartitions} closure,
 * not a {@code map} closure — so the cache is genuinely reused across every
 * message in that partition. See {@code PartitionProcessor}.
 */
public final class TermMetadataLoader implements AutoCloseable {

    /**
     * Streams a GCS object's bytes — the same functional shape as
     * {@link HyperscanDatabaseLoader.GcsByteStreamer}, kept as a separate
     * interface (rather than reusing that one directly) so this class's own
     * Javadoc/call sites read clearly as "the metadata streamer", not an
     * incidental reuse of an unrelated class's nested type.
     */
    @FunctionalInterface
    public interface GcsByteStreamer {
        InputStream openStream(String gcsPath) throws IOException;
    }

    private final Map<String, String> featureToMetadataPath;
    private final GcsByteStreamer streamer;
    private final LruCache<String, TermExpressionMetadata> cache;

    /**
     * @param featureToMetadataPath resolved feature → term-metadata JSON GCS path — the
     *                               small, broadcast-from-driver map (see
     *                               {@code HyperscanPathResolver#buildTermMetadataPath})
     * @param streamer                opens a byte stream for a GCS path
     * @param maxCachedEntries        bounds this partition's cumulative cached-metadata count —
     *                               mirrors {@link HyperscanDatabaseLoader}'s own bound; metadata
     *                               objects are small (plain id lists), so this can safely be
     *                               generous, but is still bounded rather than unbounded for the
     *                               same long-lived-executor reasoning as the database cache
     */
    public TermMetadataLoader(Map<String, String> featureToMetadataPath, GcsByteStreamer streamer,
                               int maxCachedEntries) {
        this.featureToMetadataPath = featureToMetadataPath;
        this.streamer = streamer;
        this.cache = new LruCache<>(maxCachedEntries);
    }

    /**
     * Returns the loaded {@link TermExpressionMetadata} for {@code feature},
     * loading and caching it on first request within this partition.
     *
     * @throws HyperscanPathResolver.HyperscanFileNotFoundException if {@code feature} has no
     *          resolved metadata path (mirrors {@link HyperscanDatabaseLoader#load}'s own
     *          requirement 3.a-style handling)
     * @throws HyperscanFileLoadException if the GCS stream or parse fails
     */
    public TermExpressionMetadata load(String feature) {
        return cache.computeIfAbsent(feature, this::loadFresh);
    }

    private TermExpressionMetadata loadFresh(String feature) {
        String path = featureToMetadataPath.get(feature);
        if (path == null) {
            throw new HyperscanPathResolver.HyperscanFileNotFoundException(
                    "No resolved term-metadata path for feature '" + feature + "' — it was not present "
                    + "in the driver-resolved feature-to-metadata-path map. This indicates the view "
                    + "returned a feature this job's path resolution never saw, which should not happen "
                    + "if both derive from the same view query result.");
        }
        try (InputStream in = streamer.openStream(path)) {
            String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return TermExpressionMetadata.parse(feature, json);
        } catch (IOException | RuntimeException e) {
            throw new HyperscanFileLoadException(
                    "Failed to load term metadata for feature '" + feature + "' from " + path
                    + ": " + e.getMessage(), e);
        }
    }

    /** @return how many term-metadata entries are currently cached in this partition's loader. */
    public int cachedCount() {
        return cache.size();
    }

    @Override
    public void close() {
        // No native resources to release — see HyperscanDatabaseLoader class Javadoc for the
        // same reasoning applied to Database objects; this is kept as an explicit extension
        // point for symmetry rather than silently absent.
    }
}
