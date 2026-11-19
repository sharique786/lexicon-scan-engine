package com.db.macs3.ecomms.spectre.avro;

import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link MessageAvroReader}'s own request-shaping logic — which
 * {@code restricted/}/{@code unrestricted/} GCS prefixes it checks, and the
 * "neither subfolder has any file" failure path — against a mocked
 * {@link GcsClient}, without needing a live GCS connection or a real
 * {@code SparkSession}. This particular path never reaches Spark's own
 * {@code avro} reader at all (it throws before that), so no Spark session is
 * needed here; the actual-read path is exercised indirectly wherever a real
 * Spark job runs this class (see README "Known limitations" — this project's
 * Spark-dependent classes have no dedicated unit tests of their own beyond
 * what compiles/is exercised here).
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MessageAvroReader (GcsClient mocked)")
class MessageAvroReaderTest {

    @Mock
    private GcsClient gcsClient;

    @Test
    @DisplayName("throws NoAvroFilesFoundException when neither restricted/ nor unrestricted/ has any file")
    void throwsWhenNeitherSubfolderHasFiles() {
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("coreapp-trans/ds1/restricted/"), eq(".avro")))
                .thenReturn(false);
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("coreapp-trans/ds1/unrestricted/"), eq(".avro")))
                .thenReturn(false);

        assertThatThrownBy(() -> MessageAvroReader.readDataset(
                null, gcsClient, "my-msg-bucket", "coreapp-trans", "ds1", "2026-06-18", null))
                .isInstanceOf(MessageAvroReader.NoAvroFilesFoundException.class)
                .hasMessageContaining("ds1")
                .hasMessageContaining("gs://my-msg-bucket/coreapp-trans/ds1/restricted/")
                .hasMessageContaining("gs://my-msg-bucket/coreapp-trans/ds1/unrestricted/");
    }

    @Test
    @DisplayName("treats restricted/ as empty when it only holds leftover CSV-mirror objects, not .avro files")
    void ignoresNonAvroLeftoversUnderRestricted() {
        // Simulates a rerun of this same dataset after writeRestrictedCsvMirror already wrote its
        // output under restricted/csv/ — those objects must not make hasRestrictedFiles true.
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("coreapp-trans/ds1/restricted/"), eq(".avro")))
                .thenReturn(false);
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("coreapp-trans/ds1/unrestricted/"), eq(".avro")))
                .thenReturn(false);

        assertThatThrownBy(() -> MessageAvroReader.readDataset(
                null, gcsClient, "my-msg-bucket", "coreapp-trans", "ds1", "2026-06-18", null))
                .isInstanceOf(MessageAvroReader.NoAvroFilesFoundException.class);
    }

    @Test
    @DisplayName("checks both subfolders under <msgGcsPrefix>/<datasetId>/, not a hardcoded path segment")
    void checksBothSubfoldersUnderConfiguredPrefix() {
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("custom-prefix/ds-xyz/restricted/"), eq(".avro")))
                .thenReturn(false);
        when(gcsClient.hasObjectWithSuffix(eq("my-msg-bucket"), eq("custom-prefix/ds-xyz/unrestricted/"), eq(".avro")))
                .thenReturn(false);

        assertThatThrownBy(() -> MessageAvroReader.readDataset(
                null, gcsClient, "my-msg-bucket", "custom-prefix", "ds-xyz", "2026-06-18", null))
                .isInstanceOf(MessageAvroReader.NoAvroFilesFoundException.class);
    }
}
