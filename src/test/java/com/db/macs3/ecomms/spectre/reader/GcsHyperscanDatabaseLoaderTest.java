package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.TermManifestEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("GcsHyperscanDatabaseLoader Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GcsHyperscanDatabaseLoaderTest {

    @Mock
    private Storage mockStorage;
    @Mock
    private Blob mockBlob;

    private GcsHyperscanDatabaseLoader loader;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        loader = new GcsHyperscanDatabaseLoader(mockStorage, objectMapper);
    }

    @Test @Order(1)
    @DisplayName("loadHdbBytes returns bytes for a found blob")
    void loadHdbBytes_found() {
        byte[] fakeBytes = "fake-hdb-content".getBytes(StandardCharsets.UTF_8);
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(fakeBytes);
        when(mockStorage.get("bucket1", "prefix/feature_a.hdb")).thenReturn(mockBlob);

        Map<String, byte[]> result = loader.loadHdbBytes(Set.of("feature_a"), "bucket1", "prefix");

        assertThat(result).containsKey("feature_a");
        assertThat(result.get("feature_a")).isEqualTo(fakeBytes);
    }

    @Test @Order(2)
    @DisplayName("loadHdbBytes skips a feature when the blob does not exist")
    void loadHdbBytes_notFound_skipped() {
        when(mockStorage.get("bucket1", "prefix/missing_feature.hdb")).thenReturn(null);

        Map<String, byte[]> result = loader.loadHdbBytes(Set.of("missing_feature"), "bucket1", "prefix");

        assertThat(result).doesNotContainKey("missing_feature");
        assertThat(result).isEmpty();
    }

    @Test @Order(3)
    @DisplayName("loadHdbBytes handles trailing slash in prefix without double-slashing")
    void loadHdbBytes_prefixWithTrailingSlash() {
        byte[] fakeBytes = "content".getBytes(StandardCharsets.UTF_8);
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(fakeBytes);
        when(mockStorage.get("bucket1", "prefix/feature_a.hdb")).thenReturn(mockBlob);

        Map<String, byte[]> result = loader.loadHdbBytes(Set.of("feature_a"), "bucket1", "prefix/");
        assertThat(result).containsKey("feature_a");
    }

    @Test @Order(10)
    @DisplayName("loadManifests parses a valid manifest JSON into expressionId-keyed map")
    void loadManifests_valid() throws Exception {
        String manifestJson = "[" +
                "{\"expressionId\":0,\"termId\":\"feat::1\",\"pattern\":\"(?:abc)\"}," +
                "{\"expressionId\":1,\"termId\":\"feat::3\",\"pattern\":\"(?:def)\"}" +
                "]";
        byte[] bytes = manifestJson.getBytes(StandardCharsets.UTF_8);
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(bytes);
        when(mockStorage.get("bucket1", "prefix/feat.manifest.json")).thenReturn(mockBlob);

        Map<String, Map<Integer, TermManifestEntry>> result =
                loader.loadManifests(Set.of("feat"), "bucket1", "prefix");

        assertThat(result).containsKey("feat");
        Map<Integer, TermManifestEntry> manifest = result.get("feat");
        assertThat(manifest).hasSize(2);
        assertThat(manifest.get(0).getTermId()).isEqualTo("feat::1");
        assertThat(manifest.get(1).getTermId()).isEqualTo("feat::3");
    }

    @Test @Order(11)
    @DisplayName("loadManifests skips a feature when the manifest blob does not exist")
    void loadManifests_notFound_skipped() {
        when(mockStorage.get("bucket1", "prefix/no_manifest.manifest.json")).thenReturn(null);
        Map<String, Map<Integer, TermManifestEntry>> result =
                loader.loadManifests(Set.of("no_manifest"), "bucket1", "prefix");
        assertThat(result).doesNotContainKey("no_manifest");
    }

    @Test @Order(12)
    @DisplayName("loadManifests handles malformed JSON without throwing (logs and skips)")
    void loadManifests_malformedJson_skipped() {
        byte[] badJson = "not valid json {{{".getBytes(StandardCharsets.UTF_8);
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(badJson);
        when(mockStorage.get("bucket1", "prefix/bad.manifest.json")).thenReturn(mockBlob);

        Map<String, Map<Integer, TermManifestEntry>> result =
                loader.loadManifests(Set.of("bad"), "bucket1", "prefix");

        assertThat(result).doesNotContainKey("bad");
    }

    @Test @Order(20)
    @DisplayName("totalSizeMb correctly sums byte array sizes and converts to MB")
    void totalSizeMb_correctCalculation() {
        Map<String, byte[]> hdbMap = Map.of(
                "f1", new byte[1024 * 1024],       // 1 MB
                "f2", new byte[512 * 1024]         // 0.5 MB
        );
        double result = loader.totalSizeMb(hdbMap);
        assertThat(result).isCloseTo(1.5, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test @Order(21)
    @DisplayName("totalSizeMb returns 0 for an empty map")
    void totalSizeMb_emptyMap() {
        assertThat(loader.totalSizeMb(Map.of())).isEqualTo(0.0);
    }
}
