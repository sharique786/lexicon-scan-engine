package com.db.macs3.ecomms.spectre.scanengine.gcs;

import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanBundleLoader;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
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

    private static final Pattern GS_URI_PATTERN = Pattern.compile("^gs://([^/]+)/(.+)$");

    private transient Storage storage;

    private Storage storage() {
        if (storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }
        return storage;
    }

    /** Parses {@code gs://bucket/path/to/object} into a {@link BlobId}. */
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
        return children;
    }

    /**
     * {@link HyperscanBundleLoader.GcsByteStreamer} implementation —
     * streams an object's bytes rather than fully buffering it into memory
     * first, matching {@code HyperscanBundleLoader}'s own memory-bounding
     * design (see that class's Javadoc).
     */
    public InputStream openStream(String gsUri) throws IOException {
        BlobId blobId = parseGsUri(gsUri);
        ReadChannel reader = storage().reader(blobId);
        return Channels.newInputStream(reader);
    }

    /** Reads a small object's full content as a UTF-8 string — for the {@code BqTableConfig} JSON file. */
    public String readTextFile(String gsUri) throws IOException {
        try (InputStream in = openStream(gsUri)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * Opens an output stream for writing a new object — for the
     * {@code lexicon-hit-restricted} CSV mirror. Any existing object at
     * {@code gsUri} is overwritten.
     */
    public OutputStream openWriteStream(String gsUri) {
        BlobId blobId = parseGsUri(gsUri);
        return Channels.newOutputStream(storage().writer(
                com.google.cloud.storage.BlobInfo.newBuilder(blobId).setContentType("text/csv").build()));
    }

    /** @return true iff an object exists at {@code gsUri} — used for the "no hyperscan file" / "no AVRO" checks. */
    public boolean exists(String gsUri) {
        BlobId blobId = parseGsUri(gsUri);
        Blob blob = storage().get(blobId);
        return blob != null && blob.exists();
    }

    /** Lists every object (recursively, no delimiter) under {@code prefix} — for locating AVRO files. */
    public List<String> listAllObjects(String bucket, String prefix) {
        List<String> names = new ArrayList<>();
        for (Blob blob : storage().list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
            if (!blob.isDirectory()) {
                names.add("gs://" + bucket + "/" + blob.getName());
            }
        }
        return names;
    }
}
