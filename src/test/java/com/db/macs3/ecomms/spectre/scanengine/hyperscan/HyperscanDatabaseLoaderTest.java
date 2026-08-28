package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("HyperscanDatabaseLoader")
class HyperscanDatabaseLoaderTest {

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

    @Test
    @DisplayName("only distinct features trigger a GCS stream open — repeated messages needing " +
                 "the same feature reuse the cached database")
    void cachesAcrossRepeatedLoads() {
        Map<String, String> pathMap = Map.of(
                "lexicon_market_cond-1", "gs://bucket/path/lexicon_market_cond-1.hdb",
                "lexicon_market_cond-2", "gs://bucket/path/lexicon_market_cond-2.hdb",
                "lexicon_market_cond-3", "gs://bucket/path/lexicon_market_cond-3.hdb"
        );
        byte[] dbBytes = realDbBytes();
        List<String> streamOpenCalls = new ArrayList<>();
        HyperscanDatabaseLoader.GcsByteStreamer streamer = path -> {
            streamOpenCalls.add(path);
            return new ByteArrayInputStream(dbBytes);
        };
        HyperscanDatabaseLoader loader = new HyperscanDatabaseLoader(pathMap, streamer, 10);

        // Pattern: 1,1,2,1,3 -- 5 "message" loads, only 3 distinct features.
        Database d1a = loader.load("lexicon_market_cond-1");
        Database d1b = loader.load("lexicon_market_cond-1");
        loader.load("lexicon_market_cond-2");
        Database d1c = loader.load("lexicon_market_cond-1");
        loader.load("lexicon_market_cond-3");

        assertThat(streamOpenCalls).hasSize(3);
        assertThat(d1a == d1b && d1b == d1c).isTrue();
        assertThat(loader.cachedCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("a feature with no resolved path throws HyperscanFileNotFoundException")
    void unknownFeatureThrows() {
        HyperscanDatabaseLoader loader = new HyperscanDatabaseLoader(
                Map.of(), path -> new ByteArrayInputStream(new byte[0]), 10);
        assertThatThrownBy(() -> loader.load("unknown-feature"))
                .isInstanceOf(HyperscanPathResolver.HyperscanFileNotFoundException.class);
    }

    @Test
    @DisplayName("a GCS stream failure surfaces as HyperscanFileLoadException, naming the feature and reason")
    void gcsFailureThrows() {
        Map<String, String> pathMap = Map.of("bad-feature", "gs://bucket/bad.hdb");
        HyperscanDatabaseLoader.GcsByteStreamer failingStreamer = path -> {
            throw new IOException("simulated GCS read failure");
        };
        HyperscanDatabaseLoader loader = new HyperscanDatabaseLoader(pathMap, failingStreamer, 10);
        assertThatThrownBy(() -> loader.load("bad-feature"))
                .isInstanceOf(HyperscanFileLoadException.class)
                .hasMessageContaining("bad-feature")
                .hasMessageContaining("simulated GCS read failure");
    }

    @Test
    @DisplayName("the cache stays bounded and evicted features genuinely reload from GCS")
    void boundedCacheEvictsAndReloads() {
        Map<String, String> pathMap = new HashMap<>();
        for (int i = 1; i <= 5; i++) pathMap.put("feat-" + i, "gs://bucket/feat-" + i + ".hdb");
        byte[] dbBytes = realDbBytes();
        List<String> calls = new ArrayList<>();
        HyperscanDatabaseLoader.GcsByteStreamer streamer = p -> { calls.add(p); return new ByteArrayInputStream(dbBytes); };
        HyperscanDatabaseLoader loader = new HyperscanDatabaseLoader(pathMap, streamer, 2);

        loader.load("feat-1");
        loader.load("feat-2");
        assertThat(loader.cachedCount()).isEqualTo(2);

        loader.load("feat-3"); // evicts feat-1 (LRU)
        assertThat(loader.cachedCount()).isEqualTo(2);

        loader.load("feat-1"); // must reload
        long feat1Calls = calls.stream().filter(c -> c.contains("feat-1")).count();
        assertThat(feat1Calls).isEqualTo(2);
    }
}
