package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("HyperscanBundleLoader")
class HyperscanBundleLoaderTest {

    /**
     * A real, minimal Hyperscan {@code .hdb} payload — {@link Database#load}
     * parses a specific binary format ({@code Database#save}'s own format,
     * not raw native bytes), so a placeholder byte array does not stand in
     * for one; only genuinely compiled+serialized bytes parse successfully.
     */
    private static byte[] realDbBytes() {
        try (Database database = Database.compile(new Expression("placeholder", 1))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            database.save(out);
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile/serialize test Hyperscan database: " + e.getMessage(), e);
        }
    }

    private static String simpleMetadataJson(String feature) {
        return """
            {"results": [
              {"termId": "%s::1", "compilationStatus": "PASS", "translatedPattern": ["placeholder"],
               "requiresExclusionCheck": false, "hyperscanExpressionId": 1}
            ]}
            """.formatted(feature);
    }

    /** Builds a real zip bundle: {@code <feature>.hdb} + {@code <feature>-compile-results.json} entries. */
    private static byte[] zipOf(String feature, byte[] hdbBytes, String metadataJson) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry(TermIdBuilder.hdbFileName(feature)));
                zip.write(hdbBytes);
                zip.closeEntry();

                zip.putNextEntry(new ZipEntry(TermIdBuilder.termMetadataFileName(feature)));
                zip.write(metadataJson.getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("only distinct features trigger a GCS stream open — repeated messages needing " +
                 "the same feature reuse the cached bundle (ONE download for both database AND metadata)")
    void cachesAcrossRepeatedLoads() {
        String feature1 = "lexicon_market_cond-1";
        String feature2 = "lexicon_market_cond-2";
        String feature3 = "lexicon_market_cond-3";
        Map<String, String> pathMap = Map.of(
                feature1, "gs://bucket/path/" + feature1 + ".zip",
                feature2, "gs://bucket/path/" + feature2 + ".zip",
                feature3, "gs://bucket/path/" + feature3 + ".zip"
        );
        byte[] dbBytes = realDbBytes();
        Map<String, byte[]> zipByFeature = new HashMap<>();
        zipByFeature.put(feature1, zipOf(feature1, dbBytes, simpleMetadataJson(feature1)));
        zipByFeature.put(feature2, zipOf(feature2, dbBytes, simpleMetadataJson(feature2)));
        zipByFeature.put(feature3, zipOf(feature3, dbBytes, simpleMetadataJson(feature3)));

        List<String> streamOpenCalls = new ArrayList<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            streamOpenCalls.add(path);
            String feature = path.contains(feature1) ? feature1 : path.contains(feature2) ? feature2 : feature3;
            return new ByteArrayInputStream(zipByFeature.get(feature));
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 10);

        // Pattern: 1,1,2,1,3 -- 5 "message" loads, only 3 distinct features.
        Database d1a = loader.loadDatabase(feature1);
        Database d1b = loader.loadDatabase(feature1);
        loader.loadDatabase(feature2);
        Database d1c = loader.loadDatabase(feature1);
        loader.loadDatabase(feature3);

        assertThat(streamOpenCalls)
                .as("ONE zip download per distinct feature — never re-downloaded for the metadata separately")
                .hasSize(3);
        assertThat(d1a == d1b && d1b == d1c).isTrue();
        assertThat(loader.cachedCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("loadDatabase and loadMetadata for the SAME feature reuse the SAME cached bundle, " +
                 "not two independent downloads")
    void databaseAndMetadataShareOneDownload() {
        String feature = "lexicon_market_cond-1";
        byte[] dbBytes = realDbBytes();
        byte[] zipBytes = zipOf(feature, dbBytes, simpleMetadataJson(feature));
        List<String> streamOpenCalls = new ArrayList<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            streamOpenCalls.add(path);
            return new ByteArrayInputStream(zipBytes);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(feature, "gs://bucket/" + feature + ".zip"), streamer, 10);

        Database database = loader.loadDatabase(feature);
        TermExpressionMetadata metadata = loader.loadMetadata(feature);

        assertThat(streamOpenCalls).hasSize(1);
        assertThat(database).isNotNull();
        assertThat(metadata.termCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("correctly extracts both the .hdb bytes and the metadata JSON from the same zip")
    void extractsBothEntriesCorrectly() {
        String feature = "lexicon_market_cond-9";
        byte[] dbBytes = realDbBytes();
        byte[] zipBytes = zipOf(feature, dbBytes, simpleMetadataJson(feature));
        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(feature, "gs://bucket/" + feature + ".zip"),
                path -> new ByteArrayInputStream(zipBytes), 10);

        HyperscanBundleLoader.LexiconBundle bundle = loader.load(feature);

        assertThat(bundle.database()).isNotNull();
        assertThat(bundle.metadata().feature()).isEqualTo(feature);
        assertThat(bundle.metadata().termByAnyExpressionId(1)).isNotNull();
    }

    @Test
    @DisplayName("a feature with no resolved path throws HyperscanFileNotFoundException")
    void unknownFeatureThrows() {
        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(), path -> new ByteArrayInputStream(new byte[0]), 10);
        assertThatThrownBy(() -> loader.load("unknown-feature"))
                .isInstanceOf(HyperscanPathResolver.HyperscanFileNotFoundException.class);
    }

    @Test
    @DisplayName("a GCS stream failure surfaces as HyperscanFileLoadException, naming the feature and reason")
    void gcsFailureThrows() {
        Map<String, String> pathMap = Map.of("bad-feature", "gs://bucket/bad.zip");
        HyperscanBundleLoader.GcsByteStreamer failingStreamer = path -> {
            throw new IOException("simulated GCS read failure");
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, failingStreamer, 10);
        assertThatThrownBy(() -> loader.load("bad-feature"))
                .isInstanceOf(HyperscanFileLoadException.class)
                .hasMessageContaining("bad-feature")
                .hasMessageContaining("simulated GCS read failure");
    }

    @Test
    @DisplayName("a zip missing the .hdb entry throws HyperscanFileLoadException naming what was expected")
    void missingHdbEntryThrows() {
        String feature = "lexicon_missing_hdb-1";
        byte[] zipBytes;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry(TermIdBuilder.termMetadataFileName(feature)));
                zip.write(simpleMetadataJson(feature).getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
            zipBytes = out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(feature, "gs://bucket/" + feature + ".zip"),
                path -> new ByteArrayInputStream(zipBytes), 10);

        assertThatThrownBy(() -> loader.load(feature))
                .isInstanceOf(HyperscanFileLoadException.class)
                .hasMessageContaining(TermIdBuilder.hdbFileName(feature));
    }

    @Test
    @DisplayName("a zip missing the metadata JSON entry throws HyperscanFileLoadException naming what was expected")
    void missingMetadataEntryThrows() {
        String feature = "lexicon_missing_json-1";
        byte[] dbBytes = realDbBytes();
        byte[] zipBytes;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(out)) {
                zip.putNextEntry(new ZipEntry(TermIdBuilder.hdbFileName(feature)));
                zip.write(dbBytes);
                zip.closeEntry();
            }
            zipBytes = out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(feature, "gs://bucket/" + feature + ".zip"),
                path -> new ByteArrayInputStream(zipBytes), 10);

        assertThatThrownBy(() -> loader.load(feature))
                .isInstanceOf(HyperscanFileLoadException.class)
                .hasMessageContaining(TermIdBuilder.termMetadataFileName(feature));
    }

    @Test
    @DisplayName("the cache stays bounded and evicted features genuinely re-download from GCS")
    void boundedCacheEvictsAndReloads() {
        Map<String, String> pathMap = new HashMap<>();
        Map<String, byte[]> zipByFeature = new HashMap<>();
        byte[] dbBytes = realDbBytes();
        for (int i = 1; i <= 5; i++) {
            String feature = "feat-" + i;
            pathMap.put(feature, "gs://bucket/" + feature + ".zip");
            zipByFeature.put(feature, zipOf(feature, dbBytes, simpleMetadataJson(feature)));
        }
        List<String> calls = new ArrayList<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            calls.add(path);
            for (Map.Entry<String, byte[]> e : zipByFeature.entrySet()) {
                if (path.contains(e.getKey())) {
                    return new ByteArrayInputStream(e.getValue());
                }
            }
            throw new IOException("no fixture for " + path);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 2);

        loader.load("feat-1");
        loader.load("feat-2");
        assertThat(loader.cachedCount()).isEqualTo(2);

        loader.load("feat-3"); // evicts feat-1 (LRU)
        assertThat(loader.cachedCount()).isEqualTo(2);

        loader.load("feat-1"); // must reload
        long feat1Calls = calls.stream().filter(c -> c.contains("feat-1")).count();
        assertThat(feat1Calls).isEqualTo(2);
    }

    // ── prefetch(): concurrent warm-up (JDK 21 virtual threads) ────────────────

    private static Map<String, byte[]> zipsFor(byte[] dbBytes, String... features) {
        Map<String, byte[]> zips = new HashMap<>();
        for (String feature : features) {
            zips.put(feature, zipOf(feature, dbBytes, simpleMetadataJson(feature)));
        }
        return zips;
    }

    @Test
    @DisplayName("prefetch() warms the cache for every distinct feature — subsequent load() calls are pure cache hits")
    void prefetchWarmsCacheForAllDistinctFeatures() {
        byte[] dbBytes = realDbBytes();
        String[] features = {"feat-a", "feat-b", "feat-c"};
        Map<String, byte[]> zips = zipsFor(dbBytes, features);
        Map<String, String> pathMap = new HashMap<>();
        for (String f : features) pathMap.put(f, "gs://bucket/" + f + ".zip");

        List<String> streamOpenCalls = new CopyOnWriteArrayList<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            streamOpenCalls.add(path);
            for (String f : features) {
                if (path.contains(f)) return new ByteArrayInputStream(zips.get(f));
            }
            throw new IOException("no fixture for " + path);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 10);

        loader.prefetch(List.of(features));

        assertThat(loader.cachedCount()).isEqualTo(3);
        assertThat(streamOpenCalls).hasSize(3);

        // Every load() now must be a pure cache hit — no further stream opens.
        for (String f : features) {
            loader.load(f);
        }
        assertThat(streamOpenCalls).as("load() after prefetch() must not re-download anything").hasSize(3);
    }

    @Test
    @DisplayName("prefetch() caps at the cache's own max size — never does concurrent work for " +
                 "entries that would just be evicted before ever being consulted")
    void prefetchCapsAtCacheMaxSize() {
        byte[] dbBytes = realDbBytes();
        String[] features = {"feat-1", "feat-2", "feat-3", "feat-4", "feat-5"};
        Map<String, byte[]> zips = zipsFor(dbBytes, features);
        Map<String, String> pathMap = new HashMap<>();
        for (String f : features) pathMap.put(f, "gs://bucket/" + f + ".zip");

        List<String> streamOpenCalls = new CopyOnWriteArrayList<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            streamOpenCalls.add(path);
            for (String f : features) {
                if (path.contains(f)) return new ByteArrayInputStream(zips.get(f));
            }
            throw new IOException("no fixture for " + path);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 2); // cap = 2

        loader.prefetch(List.of(features));

        assertThat(loader.cachedCount()).isEqualTo(2);
        assertThat(streamOpenCalls).as("only the first 2 (cache's own bound) should ever be downloaded").hasSize(2);
    }

    @Test
    @DisplayName("prefetch() is best-effort: a feature that fails to prefetch is silently skipped, " +
                 "never fails the call, and still loads correctly (with its real error, if any) afterward")
    void prefetchIsBestEffortOnFailure() {
        byte[] dbBytes = realDbBytes();
        String goodFeature = "feat-good";
        String badFeature = "feat-bad";
        byte[] goodZip = zipOf(goodFeature, dbBytes, simpleMetadataJson(goodFeature));

        Map<String, String> pathMap = Map.of(
                goodFeature, "gs://bucket/" + goodFeature + ".zip",
                badFeature, "gs://bucket/" + badFeature + ".zip");
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            if (path.contains(badFeature)) {
                throw new IOException("simulated GCS failure for " + badFeature);
            }
            return new ByteArrayInputStream(goodZip);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 10);

        // Must not throw, despite one of the two features always failing.
        loader.prefetch(List.of(goodFeature, badFeature));

        assertThat(loader.cachedCount()).as("only the successfully-prefetched feature is cached").isEqualTo(1);
        assertThat(loader.loadDatabase(goodFeature)).isNotNull();

        // The bad feature's real error still surfaces normally on synchronous load.
        assertThatThrownBy(() -> loader.load(badFeature))
                .isInstanceOf(HyperscanFileLoadException.class)
                .hasMessageContaining(badFeature);
    }

    @Test
    @DisplayName("prefetch() with null or empty input is a safe no-op")
    void prefetchWithEmptyInputIsNoOp() {
        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(), path -> { throw new IOException("should never be called"); }, 10);

        loader.prefetch(null);
        loader.prefetch(List.of());

        assertThat(loader.cachedCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("prefetch() deduplicates repeated feature names in its input")
    void prefetchDeduplicatesInput() {
        byte[] dbBytes = realDbBytes();
        String feature = "feat-dup";
        byte[] zip = zipOf(feature, dbBytes, simpleMetadataJson(feature));
        Map<String, Integer> callCounts = new ConcurrentHashMap<>();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            callCounts.merge(path, 1, Integer::sum);
            return new ByteArrayInputStream(zip);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(
                Map.of(feature, "gs://bucket/" + feature + ".zip"), streamer, 10);

        loader.prefetch(List.of(feature, feature, feature));

        assertThat(loader.cachedCount()).isEqualTo(1);
        assertThat(callCounts.values()).as("the same feature repeated 3x in the input must download only once")
                .containsExactly(1);
    }

    @Test
    @DisplayName("prefetch() genuinely runs concurrently — many features complete in roughly the time " +
                 "of the slowest ONE, not the sum of all of them")
    void prefetchRunsConcurrently() throws InterruptedException {
        byte[] dbBytes = realDbBytes();
        int featureCount = 8;
        long perFeatureDelayMs = 150;
        String[] features = new String[featureCount];
        Map<String, byte[]> zips = new HashMap<>();
        Map<String, String> pathMap = new HashMap<>();
        for (int i = 0; i < featureCount; i++) {
            features[i] = "feat-slow-" + i;
            zips.put(features[i], zipOf(features[i], dbBytes, simpleMetadataJson(features[i])));
            pathMap.put(features[i], "gs://bucket/" + features[i] + ".zip");
        }
        Set<String> distinctThreads = ConcurrentHashMap.newKeySet();
        HyperscanBundleLoader.GcsByteStreamer streamer = path -> {
            distinctThreads.add(Thread.currentThread().toString());
            try {
                Thread.sleep(perFeatureDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (String f : features) {
                if (path.contains(f)) return new ByteArrayInputStream(zips.get(f));
            }
            throw new IOException("no fixture for " + path);
        };
        HyperscanBundleLoader loader = new HyperscanBundleLoader(pathMap, streamer, 10);

        long start = System.nanoTime();
        loader.prefetch(List.of(features));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(loader.cachedCount()).isEqualTo(featureCount);
        assertThat(distinctThreads)
                .as("each feature's simulated GCS read observed a DIFFERENT (virtual) thread — genuine concurrency")
                .hasSize(featureCount);
        assertThat(elapsedMs)
                .as("8 features x 150ms would take ~1200ms serially — concurrent execution should stay well under that")
                .isLessThan(perFeatureDelayMs * featureCount / 2);
    }
}
