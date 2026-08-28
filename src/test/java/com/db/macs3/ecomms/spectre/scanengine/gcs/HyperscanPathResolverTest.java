package com.db.macs3.ecomms.spectre.scanengine.gcs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("HyperscanPathResolver")
class HyperscanPathResolverTest {

    @Test
    @DisplayName("resolves the base path correctly for a single matching folder")
    void resolvesSingleMatch() {
        HyperscanPathResolver.GcsDirectoryLister lister = (bucket, prefix) ->
                List.of("2026-08-16_10-00-00_101", "2026-08-15_09-00-00_202");
        String basePath = HyperscanPathResolver.resolveBasePath("my-bucket", "101", lister);
        assertThat(basePath).isEqualTo("gs://my-bucket/policy_test/2026-08-16_10-00-00_101/lex-hyperscan/");
    }

    @Test
    @DisplayName("makes exactly ONE GCS listing call, regardless of how many features need paths afterward")
    void makesExactlyOneListingCall() {
        List<String> listCalls = new ArrayList<>();
        HyperscanPathResolver.GcsDirectoryLister lister = (bucket, prefix) -> {
            listCalls.add(bucket + "/" + prefix);
            return List.of("2026-08-16_10-00-00_101");
        };
        String basePath = HyperscanPathResolver.resolveBasePath("my-bucket", "101", lister);
        HyperscanPathResolver.buildHdbPath(basePath, "feature-a");
        HyperscanPathResolver.buildHdbPath(basePath, "feature-b");
        HyperscanPathResolver.buildHdbPath(basePath, "feature-c");
        assertThat(listCalls).hasSize(1);
    }

    @Test
    @DisplayName("when multiple folders match, picks the lexicographically greatest (most recent)")
    void picksLatestOnMultipleMatches() {
        HyperscanPathResolver.GcsDirectoryLister lister = (bucket, prefix) -> List.of(
                "2026-08-14_08-00-00_101", "2026-08-16_10-00-00_101", "2026-08-15_09-00-00_101");
        String basePath = HyperscanPathResolver.resolveBasePath("my-bucket", "101", lister);
        assertThat(basePath).contains("2026-08-16_10-00-00_101");
    }

    @Test
    @DisplayName("throws HyperscanFileNotFoundException when no folder matches (requirement 3.a)")
    void throwsWhenNoMatch() {
        HyperscanPathResolver.GcsDirectoryLister lister = (bucket, prefix) -> List.of("2026-08-16_10-00-00_999");
        assertThatThrownBy(() -> HyperscanPathResolver.resolveBasePath("my-bucket", "101", lister))
                .isInstanceOf(HyperscanPathResolver.HyperscanFileNotFoundException.class)
                .hasMessageContaining("101");
    }

    @Test
    @DisplayName("buildHdbPath produces the correct .hdb filename")
    void buildsHdbPath() {
        String hdbPath = HyperscanPathResolver.buildHdbPath(
                "gs://my-bucket/policy_test/2026-08-16_10-00-00_101/lex-hyperscan/", "lexicon_market_cond-1");
        assertThat(hdbPath).isEqualTo(
                "gs://my-bucket/policy_test/2026-08-16_10-00-00_101/lex-hyperscan/lexicon_market_cond-1.hdb");
    }

    @Test
    @DisplayName("buildHdbPath rejects a basePath without a trailing slash")
    void rejectsMalformedBasePath() {
        assertThatThrownBy(() -> HyperscanPathResolver.buildHdbPath("gs://bucket/no-trailing-slash", "feature-1"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
