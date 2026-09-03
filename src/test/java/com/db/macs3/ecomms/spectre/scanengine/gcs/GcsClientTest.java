package com.db.macs3.ecomms.spectre.scanengine.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration-style test for {@link GcsClient} — the one place this project
 * actually talks to GCS (see that class Javadoc). Every real network call is
 * replaced by a Mockito mock of the {@code com.google.cloud.storage.Storage}
 * client (and the {@link ReadChannel}/{@link WriteChannel} objects it hands
 * back), injected via the package-private test constructor — no live GCS
 * connection, credentials, or emulator involved. This exercises GcsClient's
 * OWN request/response-shaping logic (which {@code BlobListOption}s are
 * passed, directory-vs-file filtering, {@code gs://} URI parsing, stream
 * wrapping) rather than Google's client library itself.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("GcsClient (Storage request/response mocked)")
class GcsClientTest {

    @Mock
    private Storage storage;

    // ── parseGsUri (pure, no Storage interaction) ───────────────────────────

    @Test
    @DisplayName("parseGsUri splits a valid gs:// URI into bucket + object name")
    void parsesValidGsUri() {
        BlobId blobId = GcsClient.parseGsUri("gs://my-bucket/some/nested/object.json");
        assertThat(blobId.getBucket()).isEqualTo("my-bucket");
        assertThat(blobId.getName()).isEqualTo("some/nested/object.json");
    }

    @Test
    @DisplayName("parseGsUri rejects a URI without the gs:// scheme")
    void rejectsNonGsUri() {
        assertThatThrownBy(() -> GcsClient.parseGsUri("https://my-bucket/object.json"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("parseGsUri rejects a bucket-only URI with no object path")
    void rejectsBucketOnlyUri() {
        assertThatThrownBy(() -> GcsClient.parseGsUri("gs://my-bucket"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── listImmediateChildDirectories ────────────────────────────────────────

    @Test
    @DisplayName("listImmediateChildDirectories returns only directory blobs, prefix and trailing slash stripped")
    void listsOnlyDirectoriesStripped() {
        Blob directoryBlob = mock(Blob.class);
        when(directoryBlob.isDirectory()).thenReturn(true);
        when(directoryBlob.getName()).thenReturn("policy_test/2026-08-16_10-00-00_101/");

        Blob fileBlobUnderSamePrefix = mock(Blob.class);
        when(fileBlobUnderSamePrefix.isDirectory()).thenReturn(false);

        Page<Blob> page = fakePage(List.of(directoryBlob, fileBlobUnderSamePrefix));
        when(storage.list(eq("my-bucket"), any(Storage.BlobListOption[].class))).thenReturn(page);

        GcsClient gcsClient = new GcsClient(storage);
        List<String> children = gcsClient.listImmediateChildDirectories("my-bucket", "policy_test/");

        assertThat(children).containsExactly("2026-08-16_10-00-00_101");
    }

    @Test
    @DisplayName("listImmediateChildDirectories returns an empty list when nothing matches the prefix")
    void listsNoDirectoriesWhenEmpty() {
        // NOTE: fakePage(...)/fakeReadChannel(...)/etc. must be built into a local variable BEFORE
        // being handed to another mock's .thenReturn(...) — building+stubbing a mock inline, as an
        // argument to a different mock's still-open when(...).thenReturn(...), corrupts Mockito's
        // in-progress-stubbing state (confirmed by reproducing exactly this "unfinished stubbing"
        // failure before fixing every call site here the same way).
        Page<Blob> emptyPage = fakePage(List.of());
        when(storage.list(eq("my-bucket"), any(Storage.BlobListOption[].class))).thenReturn(emptyPage);

        GcsClient gcsClient = new GcsClient(storage);
        assertThat(gcsClient.listImmediateChildDirectories("my-bucket", "policy_test/")).isEmpty();
    }

    // ── listAllObjects ────────────────────────────────────────────────────────

    @Test
    @DisplayName("listAllObjects returns full gs:// paths for files only, recursively, directories excluded")
    void listsAllObjectsFilesOnly() {
        Blob fileBlob = mock(Blob.class);
        when(fileBlob.isDirectory()).thenReturn(false);
        when(fileBlob.getName()).thenReturn("coreapp-trans/ds1/restricted/part-00000.avro");

        Blob directoryBlob = mock(Blob.class);
        when(directoryBlob.isDirectory()).thenReturn(true);

        Page<Blob> page = fakePage(List.of(fileBlob, directoryBlob));
        when(storage.list(eq("my-bucket"), any(Storage.BlobListOption[].class))).thenReturn(page);

        GcsClient gcsClient = new GcsClient(storage);
        List<String> objects = gcsClient.listAllObjects("my-bucket", "coreapp-trans/ds1/");

        assertThat(objects).containsExactly("gs://my-bucket/coreapp-trans/ds1/restricted/part-00000.avro");
    }

    // ── openStream / readTextFile ────────────────────────────────────────────

    @Test
    @DisplayName("readTextFile streams and decodes the mocked reader channel's bytes as UTF-8")
    void readsTextFileFromMockedReader() throws Exception {
        String expectedContent = "{\"dataset_id\":\"ds1\"}";
        ReadChannel readChannel = fakeReadChannel(expectedContent.getBytes(StandardCharsets.UTF_8));
        when(storage.reader(GcsClient.parseGsUri("gs://my-bucket/config/file.json"))).thenReturn(readChannel);

        GcsClient gcsClient = new GcsClient(storage);
        String actual = gcsClient.readTextFile("gs://my-bucket/config/file.json");

        assertThat(actual).isEqualTo(expectedContent);
    }

    @Test
    @DisplayName("openStream returns a stream reading exactly the mocked channel's bytes, even across multiple small reads")
    void opensStreamAcrossMultipleReads() throws Exception {
        byte[] content = "a longer body that will not fit in one single small internal read call".getBytes(StandardCharsets.UTF_8);
        ReadChannel readChannel = fakeReadChannel(content);
        when(storage.reader(GcsClient.parseGsUri("gs://my-bucket/big.txt"))).thenReturn(readChannel);

        GcsClient gcsClient = new GcsClient(storage);
        try (InputStream in = gcsClient.openStream("gs://my-bucket/big.txt")) {
            assertThat(in.readAllBytes()).isEqualTo(content);
        }
    }

    // ── exists ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("exists returns true when Storage.get returns a Blob reporting it exists")
    void existsTrueWhenBlobExists() {
        BlobId blobId = GcsClient.parseGsUri("gs://my-bucket/file.hdb");
        Blob blob = mock(Blob.class);
        when(blob.exists()).thenReturn(true);
        when(storage.get(blobId)).thenReturn(blob);

        assertThat(new GcsClient(storage).exists("gs://my-bucket/file.hdb")).isTrue();
    }

    @Test
    @DisplayName("exists returns false when Storage.get returns a Blob reporting it does not exist")
    void existsFalseWhenBlobDoesNotExist() {
        BlobId blobId = GcsClient.parseGsUri("gs://my-bucket/file.hdb");
        Blob blob = mock(Blob.class);
        when(blob.exists()).thenReturn(false);
        when(storage.get(blobId)).thenReturn(blob);

        assertThat(new GcsClient(storage).exists("gs://my-bucket/file.hdb")).isFalse();
    }

    @Test
    @DisplayName("exists returns false when Storage.get returns null (no such object at all)")
    void existsFalseWhenNoSuchBlob() {
        BlobId blobId = GcsClient.parseGsUri("gs://my-bucket/missing.hdb");
        when(storage.get(blobId)).thenReturn(null);

        assertThat(new GcsClient(storage).exists("gs://my-bucket/missing.hdb")).isFalse();
    }

    // ── openWriteStream ───────────────────────────────────────────────────────

    @Test
    @DisplayName("openWriteStream requests a text/csv blob at the parsed bucket/name and forwards written bytes to the channel")
    void opensWriteStreamWithCorrectBlobInfoAndForwardsBytes() throws Exception {
        ByteArrayOutputStream capturedBytes = new ByteArrayOutputStream();
        WriteChannel writeChannel = fakeWriteChannel(capturedBytes);
        ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
        when(storage.writer(blobInfoCaptor.capture())).thenReturn(writeChannel);

        GcsClient gcsClient = new GcsClient(storage);
        byte[] payload = "message_id,term_id\nm1,t1\n".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = gcsClient.openWriteStream("gs://my-bucket/restricted/out.csv")) {
            out.write(payload);
        }

        BlobInfo requestedBlobInfo = blobInfoCaptor.getValue();
        assertThat(requestedBlobInfo.getBlobId().getBucket()).isEqualTo("my-bucket");
        assertThat(requestedBlobInfo.getBlobId().getName()).isEqualTo("restricted/out.csv");
        assertThat(requestedBlobInfo.getContentType()).isEqualTo("text/csv");
        assertThat(capturedBytes.toByteArray()).isEqualTo(payload);
    }

    // ── test doubles: real byte-forwarding channels, not deep Mockito stubs ────

    @SuppressWarnings("unchecked")
    private static Page<Blob> fakePage(List<Blob> blobs) {
        Page<Blob> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(blobs);
        return page;
    }

    /**
     * A {@link ReadChannel} mock whose {@code read} answers copy real bytes out of {@code content}.
     * {@code isOpen()} deliberately NOT stubbed — {@code java.nio.channels.Channels.newInputStream}
     * never calls it, so Mockito's strict stubbing flags a stub for it as unnecessary.
     */
    private static ReadChannel fakeReadChannel(byte[] content) throws Exception {
        ReadChannel readChannel = mock(ReadChannel.class);
        AtomicInteger position = new AtomicInteger(0);
        when(readChannel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer destination = invocation.getArgument(0);
            int remaining = content.length - position.get();
            if (remaining <= 0) {
                return -1;
            }
            int toCopy = Math.min(remaining, destination.remaining());
            destination.put(content, position.get(), toCopy);
            position.addAndGet(toCopy);
            return toCopy;
        });
        return readChannel;
    }

    /**
     * A {@link WriteChannel} mock whose {@code write} answers copy real bytes into {@code sink}.
     * {@code isOpen()} deliberately NOT stubbed — {@code java.nio.channels.Channels.newOutputStream}
     * never calls it, so Mockito's strict stubbing flags a stub for it as unnecessary (confirmed by
     * reproducing the {@code UnnecessaryStubbingException} before removing it).
     */
    private static WriteChannel fakeWriteChannel(ByteArrayOutputStream sink) throws Exception {
        WriteChannel writeChannel = mock(WriteChannel.class);
        when(writeChannel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer source = invocation.getArgument(0);
            int n = source.remaining();
            byte[] tmp = new byte[n];
            source.get(tmp);
            sink.write(tmp);
            return n;
        });
        return writeChannel;
    }
}
