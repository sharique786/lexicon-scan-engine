package com.db.macs3.ecomms.spectre.hyperscan;

import com.gliwka.hyperscan.wrapper.Database;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoaderTest.realDbBytes;
import static com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoaderTest.simpleMetadataJson;
import static com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoaderTest.zipOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link HyperscanBundleLoader} backed by an executor-wide {@link SharedBundleCache} — real Hyperscan databases, so a
 * double close (which crashes on hyperscan-java 5.4.0) or a premature close would surface here.
 */
@DisplayName("HyperscanBundleLoader with a shared executor cache")
class HyperscanBundleLoaderSharedCacheTest {

    private static final String FEATURE_A = "lexicon_shared-a";
    private static final String FEATURE_B = "lexicon_shared-b";
    private static final Map<String, String> PATHS = Map.of(
            FEATURE_A, "gs://bucket/" + FEATURE_A + ".zip",
            FEATURE_B, "gs://bucket/" + FEATURE_B + ".zip");

    private final List<String> downloads = new CopyOnWriteArrayList<>();
    private final byte[] dbBytes = realDbBytes();

    private HyperscanBundleLoader.GcsByteStreamer streamer() {
        return path -> {
            downloads.add(path);
            String feature = path.contains(FEATURE_A) ? FEATURE_A : FEATURE_B;
            return new ByteArrayInputStream(zipOf(feature, dbBytes, simpleMetadataJson(feature)));
        };
    }

    private HyperscanBundleLoader loader(SharedBundleCache shared, String runId) {
        return new HyperscanBundleLoader(PATHS, streamer(), 5, shared, runId);
    }

    @Test
    @DisplayName("two partitions of one run share ONE download and ONE native database")
    void partitionsShareOneLoad() {
        SharedBundleCache shared = new SharedBundleCache(4, HyperscanBundleLoader::closeBundleQuietly);
        try (HyperscanBundleLoader first = loader(shared, "run-1");
             HyperscanBundleLoader second = loader(shared, "run-1")) {
            Database a1 = first.loadDatabase(FEATURE_A);
            Database a2 = second.loadDatabase(FEATURE_A);

            assertThat(a2).isSameAs(a1);
            assertThat(downloads).hasSize(1);
        }
        shared.closeIdle();
    }

    @Test
    @DisplayName("a closing partition does not close a database another partition still holds")
    void closingOnePartitionKeepsOthersDatabaseOpen() {
        SharedBundleCache shared = new SharedBundleCache(0, HyperscanBundleLoader::closeBundleQuietly);
        HyperscanBundleLoader first = loader(shared, "run-1");
        try (HyperscanBundleLoader second = loader(shared, "run-1")) {
            first.loadDatabase(FEATURE_A);
            Database held = second.loadDatabase(FEATURE_A);

            first.close(); // idle cap 0 — would close it right away if `second` did not still lease it

            assertThat(shared.openCount()).isEqualTo(1);
            assertThat(second.loadDatabase(FEATURE_A)).isSameAs(held);
        }
        assertThat(shared.openCount()).as("last lease released, idle cap 0 -> closed").isZero();
    }

    @Test
    @DisplayName("a later partition reuses a bundle an earlier, finished partition left idle")
    void laterPartitionReusesIdleBundle() {
        SharedBundleCache shared = new SharedBundleCache(4, HyperscanBundleLoader::closeBundleQuietly);
        try (HyperscanBundleLoader first = loader(shared, "run-1")) {
            first.loadDatabase(FEATURE_A);
        }
        assertThat(shared.idleCount()).isEqualTo(1);

        try (HyperscanBundleLoader later = loader(shared, "run-1")) {
            later.loadDatabase(FEATURE_A);
        }
        assertThat(downloads).hasSize(1);
        shared.closeIdle();
        assertThat(shared.openCount()).isZero();
    }

    @Test
    @DisplayName("different runs never share a bundle, even for the same zip path")
    void differentRunsDoNotShare() {
        SharedBundleCache shared = new SharedBundleCache(4, HyperscanBundleLoader::closeBundleQuietly);
        try (HyperscanBundleLoader run1 = loader(shared, "run-1");
             HyperscanBundleLoader run2 = loader(shared, "run-2")) {
            assertThat(run2.loadDatabase(FEATURE_A)).isNotSameAs(run1.loadDatabase(FEATURE_A));
            assertThat(downloads).hasSize(2);
        }
        shared.closeIdle();
    }

    @Test
    @DisplayName("LRU eviction inside one partition releases its lease instead of closing a shared database")
    void localEvictionReleasesLease() {
        SharedBundleCache shared = new SharedBundleCache(4, HyperscanBundleLoader::closeBundleQuietly);
        try (HyperscanBundleLoader small = new HyperscanBundleLoader(PATHS, streamer(), 1, shared, "run-1");
             HyperscanBundleLoader other = loader(shared, "run-1")) {
            Database a = other.loadDatabase(FEATURE_A);
            small.loadDatabase(FEATURE_A);
            small.loadDatabase(FEATURE_B); // evicts A from `small` (cap 1)

            assertThat(other.loadDatabase(FEATURE_A)).isSameAs(a);
            assertThat(shared.openCount()).isEqualTo(2);
        }
        shared.closeIdle();
    }

    @Test
    @DisplayName("prefetch through the shared cache warms both features and later loads hit the cache")
    void prefetchUsesSharedCache() {
        SharedBundleCache shared = new SharedBundleCache(4, HyperscanBundleLoader::closeBundleQuietly);
        try (HyperscanBundleLoader loader = loader(shared, "run-1")) {
            loader.prefetch(List.of(FEATURE_A, FEATURE_B, FEATURE_A));
            assertThat(loader.cachedCount()).isEqualTo(2);

            loader.loadDatabase(FEATURE_A);
            loader.loadDatabase(FEATURE_B);
            assertThat(downloads).hasSize(2);
        }
        shared.closeIdle();
    }
}
