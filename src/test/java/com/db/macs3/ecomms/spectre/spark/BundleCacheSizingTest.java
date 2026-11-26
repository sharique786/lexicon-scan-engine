package com.db.macs3.ecomms.spectre.spark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("BundleCacheSizing")
class BundleCacheSizingTest {

    @Test
    @DisplayName("shared cache: both caches sized to the run's DISTINCT bundles (163 distinct, ~110 per message)")
    void sharedSizesToDistinctBundles() {
        BundleCacheSizing s = BundleCacheSizing.resolve(50, 32, true, true, 110, 163);

        assertThat(s.perPartition()).isEqualTo(163);
        assertThat(s.sharedIdle()).isEqualTo(163);
        assertThat(BundleCacheSizing.thrashes(s.perPartition(), 110)).isFalse();
        assertThat(BundleCacheSizing.churns(s.sharedIdle(), 163)).isFalse();
    }

    @Test
    @DisplayName("shared cache: 500 distinct bundles in prod -> 500, no per-environment config needed")
    void sharedScalesToProdCatalogue() {
        BundleCacheSizing s = BundleCacheSizing.resolve(50, 32, true, true, 164, 500);

        assertThat(s.perPartition()).isEqualTo(500);
        assertThat(s.sharedIdle()).isEqualTo(500);
    }

    @Test
    @DisplayName("per-message count wins if larger than the distinct count (inconsistent inputs stay safe)")
    void perMessageWinsIfLarger() {
        BundleCacheSizing s = BundleCacheSizing.resolve(20, 32, true, true, 164, 0);

        assertThat(s.perPartition()).isEqualTo(164);
        assertThat(s.sharedIdle()).isEqualTo(164);
    }

    @Test
    @DisplayName("auto-size never lowers configured values")
    void neverLowers() {
        BundleCacheSizing s = BundleCacheSizing.resolve(600, 700, true, true, 5, 163);

        assertThat(s.perPartition()).isEqualTo(600);
        assertThat(s.sharedIdle()).isEqualTo(700);
    }

    @Test
    @DisplayName("auto-size off keeps configured values; thrash and churn are reported")
    void autoSizeOff() {
        BundleCacheSizing s = BundleCacheSizing.resolve(50, 32, true, false, 164, 300);

        assertThat(s.perPartition()).isEqualTo(50);
        assertThat(s.sharedIdle()).isEqualTo(32);
        assertThat(BundleCacheSizing.thrashes(s.perPartition(), 164)).isTrue();
        assertThat(BundleCacheSizing.churns(s.sharedIdle(), 300)).isTrue();
    }

    @Test
    @DisplayName("shared cache disabled: private copies, so only one message's features, never the distinct count")
    void sharedDisabled() {
        BundleCacheSizing s = BundleCacheSizing.resolve(20, 32, false, true, 100, 500);

        assertThat(s.perPartition()).isEqualTo(100);
        assertThat(s.sharedIdle()).isNegative();
        assertThat(BundleCacheSizing.churns(s.sharedIdle(), 500)).isFalse();
    }
}
