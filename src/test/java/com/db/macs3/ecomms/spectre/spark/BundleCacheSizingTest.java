package com.db.macs3.ecomms.spectre.spark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("BundleCacheSizing")
class BundleCacheSizingTest {

    @Test
    @DisplayName("auto-size raises both caches to one message's feature count (164 features, cache 20/32)")
    void raisesToWorkingSet() {
        BundleCacheSizing s = BundleCacheSizing.resolve(20, 32, true, true, 164);

        assertThat(s.perPartition()).isEqualTo(164);
        assertThat(s.sharedIdle()).isEqualTo(164);
        assertThat(BundleCacheSizing.thrashes(s.perPartition(), 164)).isFalse();
    }

    @Test
    @DisplayName("auto-size never lowers configured values")
    void neverLowers() {
        BundleCacheSizing s = BundleCacheSizing.resolve(50, 64, true, true, 5);

        assertThat(s.perPartition()).isEqualTo(50);
        assertThat(s.sharedIdle()).isEqualTo(64);
    }

    @Test
    @DisplayName("auto-size off keeps configured values, and the cycle-longer-than-cache case is reported")
    void autoSizeOff() {
        BundleCacheSizing s = BundleCacheSizing.resolve(50, 32, true, false, 164);

        assertThat(s.perPartition()).isEqualTo(50);
        assertThat(s.sharedIdle()).isEqualTo(32);
        assertThat(BundleCacheSizing.thrashes(s.perPartition(), 164)).isTrue();
    }

    @Test
    @DisplayName("shared cache disabled: idle size is negative (private loads), per-partition still sized")
    void sharedDisabled() {
        BundleCacheSizing s = BundleCacheSizing.resolve(20, 32, false, true, 100);

        assertThat(s.perPartition()).isEqualTo(100);
        assertThat(s.sharedIdle()).isNegative();
    }
}
