package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("TermIdBuilder")
class TermIdBuilderTest {

    @Test
    @DisplayName("build() produces <feature>::<index>, matching the confirmed example exactly")
    void buildsCorrectFormat() {
        assertThat(TermIdBuilder.build("lexicon_market_cond-1", 1)).isEqualTo("lexicon_market_cond-1::1");
    }

    @Test
    @DisplayName("build() preserves the feature name verbatim, hyphens included")
    void preservesFeatureVerbatim() {
        assertThat(TermIdBuilder.build("lexicon_market_cond-4", 10)).isEqualTo("lexicon_market_cond-4::10");
    }

    @Test
    @DisplayName("build() rejects a null/blank feature")
    void rejectsBlankFeature() {
        assertThatThrownBy(() -> TermIdBuilder.build(null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TermIdBuilder.build("", 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("hdbFileName() appends .hdb to the feature name")
    void buildsHdbFileName() {
        assertThat(TermIdBuilder.hdbFileName("lexicon_market_cond-1")).isEqualTo("lexicon_market_cond-1.hdb");
    }

    @Test
    @DisplayName("hdbFileName() rejects a null/blank feature")
    void hdbFileNameRejectsBlankFeature() {
        assertThatThrownBy(() -> TermIdBuilder.hdbFileName(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
