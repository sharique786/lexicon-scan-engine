package com.db.macs3.ecomms.spectre.gcs;

import com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thin wrapper over the real {@code com.google.cloud.storage} client,
 * implementing the small functional interfaces {@code HyperscanPathResolver}
 * and {@code HyperscanBundleLoader} depend on ({@link HyperscanPathResolver.GcsDirectoryLister},
 * {@link HyperscanBundleLoader.GcsByteStreamer}) so those classes' own
 * logic stays testable without a live GCS connection — this class is the one
 * place that actually talks to GCS.
 *
 * <h2>Spring-managed on the driver; plain {@code new} on executors</h2>
 * <p>{@code @Component}-annotated so the driver can receive a shared instance
 * via constructor injection. Executor-side code ({@code PartitionProcessor},
 * running inside a {@code mapPartitions} closure with no Spring
 * {@code ApplicationContext} available) instead constructs its own instance
 * directly with {@code new GcsClient()}, which works identically either way
 * since this class holds no Spring-specific state — only its {@code transient}
 * lazily-initialised {@link Storage} client.
 *
 * <p>{@link Serializable} — a {@code GcsClient} instance is constructed and
 * used from within executor-side {@code mapPartitions} closures (via
 * {@link HyperscanBundleLoader}), so it must survive Spark's task
 * serialization. The underlying {@link Storage} client itself is created
 * lazily ({@code transient} + null-check-and-construct) rather than eagerly
 * held as a serialized field, since a live client handle is not meaningfully
 * serializable — each executor JVM builds its own on first use.
 */
@Component
public final class GcsClient implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(GcsClient.class);

    private static final Pattern GS_URI_PATTERN = Pattern.compile("^gs://([^/]+)/(.+)$");

    private transient Storage storage;

    /**
     * Default constructor — the real, lazily-initialised {@link Storage} client (production/Spring use).
     */
    public GcsClient() {
    }

    /**
     * Test-only constructor — injects a pre-built {@link Storage} (a Mockito mock in every
     * current caller) so this class's own request/response-shaping logic (path parsing, which
     * {@code BlobListOption}s are passed, directory-vs-file filtering, stream wrapping) can be
     * exercised without a live GCS connection. Package-private: only {@code GcsClientTest}, in
     * this same package, uses it.
     */
    GcsClient(Storage storage) {
        this.storage = storage;
    }

    private Storage storage() {
        if (storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }
        return storage;
    }

    /**
     * Parses {@code gs://bucket/path/to/object} into a {@link BlobId}.
     */
    public static BlobId parseGsUri(String gsUri) {
        Matcher matcher = GS_URI_PATTERN.matcher(gsUri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a valid gs:// URI: " + gsUri);
        }
        return BlobId.of(matcher.group(1), matcher.group(2));
    }

    /**
     * {@link HyperscanPathResolver.GcsDirectoryLister} implementation — lists
     * the immediate child "directories" one level under {@code prefix} using
     * GCS's delimiter-based listing (the standard way to get directory-like
     * grouping from a flat, prefix-based object store).
     */
    public List<String> listImmediateChildDirectories(String bucket, String prefix) {
        log.debug("Listing immediate child directories under gs://{}/{}", bucket, prefix);
        Instant callStart = Instant.now();
        try {
            List<String> children = new ArrayList<>();
            for (Blob blob : storage().list(bucket,
                    Storage.BlobListOption.prefix(prefix),
                    Storage.BlobListOption.currentDirectory()).iterateAll()) {
                if (blob.isDirectory()) {
                    String blobName = blob.getName(); // e.g. "policy_test/2026-08-16_10-00-00_101/"
                    String childName = blobName.substring(prefix.length());
                    if (childName.endsWith("/")) {
                        childName = childName.substring(0, childName.length() - 1);
                    }
                    if (!childName.isEmpty()) {
                        children.add(childName);
                    }
                }
            }
            log.debug("Found {} child director(y/ies) under gs://{}/{} in {}ms",
                    children.size(), bucket, prefix, Duration.between(callStart, Instant.now()).toMillis());
            return children;
        } catch (StorageException e) {
            log.error("Failed to list child directories under gs://{}/{}: {}", bucket, prefix, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * {@link HyperscanBundleLoader.GcsByteStreamer} implementation —
     * streams an object's bytes rather than fully buffering it into memory
     * first, matching {@code HyperscanBundleLoader}'s own memory-bounding
     * design (see that class's Javadoc).
     */
    public InputStream openStream(String gsUri) throws IOException {
        BlobId blobId = parseGsUri(gsUri);
        try {
            ReadChannel reader = storage().reader(blobId);
            return Channels.newInputStream(reader);
        } catch (StorageException e) {
            // Wrapped into the checked IOException this method already declares — a raw
            // StorageException (unchecked) thrown from here would otherwise silently bypass
            // that declared contract, and callers up the stack (HyperscanBundleLoader,
            // readTextFile below) already handle IOException uniformly.
            log.error("Failed to open a read stream for {}: {}", gsUri, e.getMessage(), e);
            throw new IOException("Failed to open a read stream for " + gsUri, e);
        }
    }

    /**
     * Reads a small object's full content as a UTF-8 string — for the {@code BqTableConfig} JSON file.
     */
    public String readTextFile(String gsUri) throws IOException {
        log.info("Reading text file {}", gsUri);
        Instant readStart = Instant.now();
        try (InputStream in = openStream(gsUri)) {
            String content = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            log.info("Read {} bytes from {} in {}ms",
                    content.length(), gsUri, Duration.between(readStart, Instant.now()).toMillis());
            return content;
        } catch (IOException e) {
            log.error("Failed to read text file {}: {}", gsUri, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Opens an output stream for writing a new object — for the
     * {@code lexicon-hit-restricted} CSV mirror. Any existing object at
     * {@code gsUri} is overwritten.
     */
    public OutputStream openWriteStream(String gsUri) {
        BlobId blobId = parseGsUri(gsUri);
        try {
            return Channels.newOutputStream(storage().writer(
                    com.google.cloud.storage.BlobInfo.newBuilder(blobId).setContentType("text/csv").build()));
        } catch (StorageException e) {
            log.error("Failed to open a write stream for {}: {}", gsUri, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * @return true iff an object exists at {@code gsUri} — used for the "no hyperscan file" / "no AVRO" checks.
     */
    public boolean exists(String gsUri) {
        BlobId blobId = parseGsUri(gsUri);
        try {
            Blob blob = storage().get(blobId);
            return blob != null && blob.exists();
        } catch (StorageException e) {
            log.error("Failed to check existence of {}: {}", gsUri, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Lists every object (recursively, no delimiter) under {@code prefix} — for locating AVRO files.
     */
    public List<String> listAllObjects(String bucket, String prefix) {
        log.debug("Listing all objects under gs://{}/{}", bucket, prefix);
        Instant callStart = Instant.now();
        try {
            List<String> names = new ArrayList<>();
            for (Blob blob : storage().list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
                if (!blob.isDirectory()) {
                    names.add("gs://" + bucket + "/" + blob.getName());
                }
            }
            log.debug("Found {} object(s) under gs://{}/{} in {}ms",
                    names.size(), bucket, prefix, Duration.between(callStart, Instant.now()).toMillis());
            return names;
        } catch (StorageException e) {
            log.error("Failed to list objects under gs://{}/{}: {}", bucket, prefix, e.getMessage(), e);
            throw e;
        }
    }
}
