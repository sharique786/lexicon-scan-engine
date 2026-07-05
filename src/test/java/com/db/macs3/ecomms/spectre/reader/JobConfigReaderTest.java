package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.JobConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("JobConfigReader Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JobConfigReaderTest {

    @Mock
    private Storage mockStorage;
    @Mock
    private Blob mockBlob;

    private JobConfigReader reader;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        reader = new JobConfigReader(mockStorage, objectMapper);
    }

    private static final String SAMPLE_JSON =
            "{" +
            "\"bqProject\":\"my-project\"," +
            "\"bqDataset\":\"my_dataset\"," +
            "\"viewName\":\"v_test\"," +
            "\"hdbGcsBucket\":\"hdb-bucket\"," +
            "\"hdbGcsPrefix\":\"hyperscan\"," +
            "\"msgGcsBucket\":\"msg-bucket\"," +
            "\"msgGcsPrefix\":\"messages\"," +
            "\"inputTables\":{\"languageFeatureDecision\":\"proj.ds.lfd\",\"featureMaster\":\"proj.ds.fm\"}," +
            "\"outputTables\":{\"lexiconHitSummary\":\"proj.ds.lhs\"}" +
            "}";

    @Test @Order(1)
    @DisplayName("load() parses a valid JSON config from GCS")
    void load_validJson() throws IOException {
        byte[] bytes = SAMPLE_JSON.getBytes(StandardCharsets.UTF_8);
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(bytes);
        when(mockStorage.get(BlobId.of("my-bucket", "config/scan-engine.json"))).thenReturn(mockBlob);

        JobConfig config = reader.load("gs://my-bucket/config/scan-engine.json");

        assertThat(config.getBqProject()).isEqualTo("my-project");
        assertThat(config.getBqDataset()).isEqualTo("my_dataset");
        assertThat(config.getViewName()).isEqualTo("v_test");
        assertThat(config.getHdbGcsBucket()).isEqualTo("hdb-bucket");
        assertThat(config.getInputTables().languageFeatureDecision).isEqualTo("proj.ds.lfd");
        assertThat(config.getOutputTables().lexiconHitSummary).isEqualTo("proj.ds.lhs");
    }

    @Test @Order(2)
    @DisplayName("load() throws IOException when the blob does not exist")
    void load_blobNotFound_throws() {
        when(mockStorage.get(BlobId.of("my-bucket", "config/missing.json"))).thenReturn(null);
        assertThatThrownBy(() -> reader.load("gs://my-bucket/config/missing.json"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not found");
    }

    @Test @Order(3)
    @DisplayName("load() uses default values for output table names not present in the JSON")
    void load_usesDefaultsForMissingFields() throws IOException {
        String minimalJson = "{\"bqProject\":\"p\",\"bqDataset\":\"d\"}";
        when(mockBlob.exists()).thenReturn(true);
        when(mockBlob.getContent()).thenReturn(minimalJson.getBytes(StandardCharsets.UTF_8));
        when(mockStorage.get(BlobId.of("b", "c.json"))).thenReturn(mockBlob);

        JobConfig config = reader.load("gs://b/c.json");

        // Defaults from JobConfig's field initializers should still apply
        assertThat(config.getOutputTables().lexiconHitSummary).isEqualTo("spectre-audit.lexicon-hit-summary");
        assertThat(config.getViewName()).isEqualTo("v_lexicon_scan_engine_input");
    }

    // ── gs:// path parsing ────────────────────────────────────────────────────

    @Test @Order(10)
    @DisplayName("GcsPath.parse correctly splits bucket and object")
    void gcsPathParse_correct() {
        JobConfigReader.GcsPath path = JobConfigReader.GcsPath.parse("gs://my-bucket/some/nested/path.json");
        assertThat(path.bucket).isEqualTo("my-bucket");
        assertThat(path.object).isEqualTo("some/nested/path.json");
    }

    @Test @Order(11)
    @DisplayName("GcsPath.parse rejects a non-gs:// URI")
    void gcsPathParse_rejectsInvalidScheme() {
        assertThatThrownBy(() -> JobConfigReader.GcsPath.parse("https://example.com/file.json"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test @Order(12)
    @DisplayName("GcsPath.parse rejects a URI missing the object path")
    void gcsPathParse_rejectsMissingObjectPath() {
        assertThatThrownBy(() -> JobConfigReader.GcsPath.parse("gs://bucket-only"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
