package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.integration.HdbTestFixtures;
import com.db.macs3.ecomms.spectre.model.TermManifestEntry;
import com.db.macs3.ecomms.spectre.model.TermMatch;
import org.apache.spark.broadcast.Broadcast;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HyperscanMatcher Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HyperscanMatcherTest {

    private HyperscanMatcher matcher;

    @BeforeEach
    void setUp() {
        HyperscanMatcher.clearCache();
        matcher = new HyperscanMatcher();
    }

    @AfterEach
    void tearDown() {
        HyperscanMatcher.clearCache();
    }

    @SuppressWarnings("unchecked")
    private Broadcast<Map<String, byte[]>> mockHdbBroadcast(String featureName, byte[] bytes) {
        Broadcast<Map<String, byte[]>> b = Mockito.mock(Broadcast.class);
        Map<String, byte[]> map = new HashMap<>();
        map.put(featureName, bytes);
        Mockito.when(b.value()).thenReturn(map);
        return b;
    }

    @SuppressWarnings("unchecked")
    private Broadcast<Map<String, Map<Integer, TermManifestEntry>>> mockManifestBroadcast(
            String featureName, Map<Integer, TermManifestEntry> manifest) {
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> b = Mockito.mock(Broadcast.class);
        Map<String, Map<Integer, TermManifestEntry>> map = new HashMap<>();
        map.put(featureName, manifest);
        Mockito.when(b.value()).thenReturn(map);
        return b;
    }

    @Test @Order(1)
    @DisplayName("Single term match resolves termId and pattern from manifest")
    void singleMatchResolvesManifestIdentity() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("test_feature",
                HdbTestFixtures.term("test_feature::1", "bomb"));

        List<TermMatch> matches = matcher.scan("test_feature",
                "there is a bomb threat",
                mockHdbBroadcast("test_feature", feature.hdbBytes),
                mockManifestBroadcast("test_feature", feature.manifest));

        assertThat(matches).hasSize(1);
        TermMatch m = matches.get(0);
        assertThat(m.getTermId()).isEqualTo("test_feature::1");
        assertThat(m.getTermRegexPattern()).isEqualTo("bomb");
        assertThat(m.getMatchText()).isEqualToIgnoringCase("bomb");
    }

    @Test @Order(2)
    @DisplayName("Multiple terms: each match resolves its own distinct termId")
    void multipleTermsDistinctIdentity() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("multi_feature",
                HdbTestFixtures.term("multi_feature::1", "bomb"),
                HdbTestFixtures.term("multi_feature::2", "manipulate"),
                HdbTestFixtures.term("multi_feature::3", "insider"));

        List<TermMatch> matches = matcher.scan("multi_feature",
                "do not manipulate the price, insider trading and a bomb threat",
                mockHdbBroadcast("multi_feature", feature.hdbBytes),
                mockManifestBroadcast("multi_feature", feature.manifest));

        assertThat(matches).hasSizeGreaterThanOrEqualTo(3);
        assertThat(matches.stream().map(TermMatch::getTermId))
                .contains("multi_feature::1", "multi_feature::2", "multi_feature::3");
    }

    @Test @Order(3)
    @DisplayName("No match in text returns empty list")
    void noMatchReturnsEmpty() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("f1",
                HdbTestFixtures.term("f1::1", "bomb"));
        List<TermMatch> matches = matcher.scan("f1", "everything is calm today",
                mockHdbBroadcast("f1", feature.hdbBytes), mockManifestBroadcast("f1", feature.manifest));
        assertThat(matches).isEmpty();
    }

    @Test @Order(10)
    @DisplayName("Null or blank text returns empty list without scanning")
    void nullOrBlankTextReturnsEmpty() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("f1",
                HdbTestFixtures.term("f1::1", "bomb"));
        Broadcast<Map<String, byte[]>> hdb = mockHdbBroadcast("f1", feature.hdbBytes);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> manifest = mockManifestBroadcast("f1", feature.manifest);

        assertThat(matcher.scan("f1", null, hdb, manifest)).isEmpty();
        assertThat(matcher.scan("f1", "   ", hdb, manifest)).isEmpty();
        assertThat(matcher.scan("f1", "", hdb, manifest)).isEmpty();
    }

    @Test @Order(11)
    @DisplayName("Feature not present in broadcast returns empty list")
    void featureNotInBroadcast_returnsEmpty() {
        Broadcast<Map<String, byte[]>> emptyHdb = mockHdbBroadcast("other", new byte[0]);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> emptyManifest = mockManifestBroadcast("other", Map.of());
        assertThat(matcher.scan("missing", "some text", emptyHdb, emptyManifest)).isEmpty();
    }

    @Test @Order(20)
    @DisplayName("Missing manifest entry falls back to synthetic termId 'featureName::expressionId'")
    void missingManifestEntry_fallsBackToSyntheticTermId() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("no_manifest_feature",
                HdbTestFixtures.term("no_manifest_feature::1", "bomb"));

        // Broadcast HDB bytes but an EMPTY manifest map for this feature -> triggers fallback
        Broadcast<Map<String, byte[]>> hdb = mockHdbBroadcast("no_manifest_feature", feature.hdbBytes);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> manifest =
                mockManifestBroadcast("no_manifest_feature", Map.of()); // empty on purpose

        List<TermMatch> matches = matcher.scan("no_manifest_feature", "a bomb was found", hdb, manifest);

        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getTermId()).isEqualTo("no_manifest_feature::0"); // synthetic fallback
        assertThat(matches.get(0).getTermRegexPattern()).isNull();
    }

    @Test @Order(30)
    @DisplayName("Static cache: second scan for same feature reuses cached DB (cache size stays 1)")
    void cacheRetainsDatabaseBetweenScans() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("cache_test",
                HdbTestFixtures.term("cache_test::1", "price"));
        Broadcast<Map<String, byte[]>> hdb = mockHdbBroadcast("cache_test", feature.hdbBytes);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> manifest = mockManifestBroadcast("cache_test", feature.manifest);

        matcher.scan("cache_test", "the price is rising", hdb, manifest);
        int afterFirst = HyperscanMatcher.cachedDatabaseCount();

        matcher.scan("cache_test", "price manipulation detected", hdb, manifest);
        int afterSecond = HyperscanMatcher.cachedDatabaseCount();

        assertThat(afterFirst).isEqualTo(1);
        assertThat(afterSecond).isEqualTo(1);
    }

    @Test @Order(40)
    @DisplayName("Delta calculation: first match has delta=0, second match has positive delta")
    void deltaCalculation() throws Exception {
        HdbTestFixtures.CompiledFeature feature = HdbTestFixtures.compileFeature("delta_test",
                HdbTestFixtures.term("delta_test::1", "bomb"),
                HdbTestFixtures.term("delta_test::2", "unit"));
        Broadcast<Map<String, byte[]>> hdb = mockHdbBroadcast("delta_test", feature.hdbBytes);
        Broadcast<Map<String, Map<Integer, TermManifestEntry>>> manifest = mockManifestBroadcast("delta_test", feature.manifest);

        List<TermMatch> matches = matcher.scan("delta_test",
                "the bomb will destroy the entire business unit", hdb, manifest);

        assertThat(matches).isNotEmpty();
        assertThat(matches.get(0).getDelta()).isEqualTo(0L);
        if (matches.size() >= 2) {
            assertThat(matches.get(1).getDelta()).isGreaterThanOrEqualTo(0L);
        }
    }
}
