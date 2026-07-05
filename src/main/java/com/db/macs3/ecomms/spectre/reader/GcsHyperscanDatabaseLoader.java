package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.TermManifestEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Loads pre-compiled Intel Hyperscan database (.hdb) files AND their
 * companion manifest files from Google Cloud Storage.
 *
 * <h2>Why bytes and not Database objects?</h2>
 * <p>{@code com.gliwka.hyperscan.wrapper.Database} is a JNI object containing
 * a pointer to native C memory. It is NOT Java-serializable and cannot be
 * placed in a Spark broadcast variable. The correct pattern is:
 * <ol>
 *   <li>Driver: load raw {@code .hdb} bytes here</li>
 *   <li>Driver: broadcast {@code Map<featureName, byte[]>} to executors</li>
 *   <li>Executor: deserialize via {@code Database.load(InputStream)},
 *       cached in a static executor-level map (see
 *       {@link com.db.macs3.ecomms.spectre.engine.HyperscanMatcher})</li>
 * </ol>
 *
 * <h2>Manifest files</h2>
 * <p>Each feature also has a {@code <featureName>.manifest.json} file at the
 * same GCS prefix, mapping Hyperscan's numeric {@code expressionId} back to
 * the human-readable {@code termId} and original PCRE pattern — see
 * {@link TermManifestEntry} for the full rationale. This loader fetches both
 * files together so the two stay consistent per feature.
 */
@Component
public class GcsHyperscanDatabaseLoader {

    private static final Logger log = LoggerFactory.getLogger(GcsHyperscanDatabaseLoader.class);

    private final Storage storageClient;
    private final ObjectMapper objectMapper;

    public GcsHyperscanDatabaseLoader() {
        this.storageClient = StorageOptions.getDefaultInstance().getService();
        this.objectMapper  = new ObjectMapper();
    }

    /** Package-private constructor for unit tests (allows injecting mocks). */
    GcsHyperscanDatabaseLoader(Storage storageClient, ObjectMapper objectMapper) {
        this.storageClient = storageClient;
        this.objectMapper  = objectMapper;
    }

    /**
     * Loads the raw bytes of each .hdb file whose name appears in {@code featureNames}.
     *
     * @param featureNames set of feature names (without .hdb extension) to load
     * @param gcsBucket    GCS bucket name (without gs://)
     * @param gcsPrefix    GCS key prefix (without trailing slash)
     * @return map of featureName -> raw .hdb file bytes for all files found
     */
    public Map<String, byte[]> loadHdbBytes(Set<String> featureNames, String gcsBucket, String gcsPrefix) {
        log.info("Loading {} .hdb files from gs://{}/{}/", featureNames.size(), gcsBucket, gcsPrefix);
        Map<String, byte[]> result = new HashMap<>(featureNames.size() * 2);
        String prefix = gcsPrefix.endsWith("/") ? gcsPrefix : gcsPrefix + "/";

        for (String featureName : featureNames) {
            String blobName = prefix + featureName + ".hdb";
            Blob blob = storageClient.get(gcsBucket, blobName);
            if (blob == null || !blob.exists()) {
                log.warn("HDB file not found for feature '{}' at gs://{}/{} — this feature will NOT be scanned",
                         featureName, gcsBucket, blobName);
                continue;
            }
            byte[] bytes = blob.getContent();
            result.put(featureName, bytes);
            log.info("Loaded HDB for '{}': {} bytes ({} KB)", featureName, bytes.length, bytes.length / 1024);
        }
        log.info("HDB load complete: {}/{} files loaded successfully", result.size(), featureNames.size());
        return result;
    }

    /**
     * Loads and parses the {@code <featureName>.manifest.json} file for each
     * feature name, giving {@code Map<expressionId, TermManifestEntry>} per feature.
     *
     * <p>A feature with no manifest file logs a warning and is simply absent
     * from the result map — {@code HyperscanMatcher} treats missing manifests
     * gracefully by falling back to a synthetic {@code termId} of
     * {@code featureName + "::" + expressionId} (see its Javadoc).
     *
     * @param featureNames set of feature names to load manifests for
     * @param gcsBucket    GCS bucket name (same bucket as the .hdb files)
     * @param gcsPrefix    GCS key prefix (same prefix as the .hdb files)
     * @return map of featureName -> (expressionId -> TermManifestEntry)
     */
    public Map<String, Map<Integer, TermManifestEntry>> loadManifests(Set<String> featureNames,
                                                                        String gcsBucket,
                                                                        String gcsPrefix) {
        log.info("Loading {} manifest files from gs://{}/{}/", featureNames.size(), gcsBucket, gcsPrefix);
        Map<String, Map<Integer, TermManifestEntry>> result = new HashMap<>(featureNames.size() * 2);
        String prefix = gcsPrefix.endsWith("/") ? gcsPrefix : gcsPrefix + "/";

        for (String featureName : featureNames) {
            String blobName = prefix + featureName + ".manifest.json";
            Blob blob = storageClient.get(gcsBucket, blobName);
            if (blob == null || !blob.exists()) {
                log.warn("Manifest file not found for feature '{}' at gs://{}/{} — " +
                         "term_id/pattern will fall back to synthetic values",
                         featureName, gcsBucket, blobName);
                continue;
            }
            try {
                byte[] bytes = blob.getContent();
                String json  = new String(bytes, StandardCharsets.UTF_8);
                TermManifestEntry[] entries = objectMapper.readValue(json, TermManifestEntry[].class);

                Map<Integer, TermManifestEntry> byExpressionId = new HashMap<>(entries.length * 2);
                for (TermManifestEntry entry : entries) {
                    byExpressionId.put(entry.getExpressionId(), entry);
                }
                result.put(featureName, byExpressionId);
                log.info("Loaded manifest for '{}': {} terms", featureName, entries.length);
            } catch (IOException e) {
                log.error("Failed to parse manifest for '{}': {}", featureName, e.getMessage(), e);
            }
        }
        log.info("Manifest load complete: {}/{} manifests loaded", result.size(), featureNames.size());
        return result;
    }

    /**
     * @return total in-memory size of all loaded .hdb bytes, in megabytes.
     *         Used to decide whether streaming (rather than broadcasting) is needed.
     */
    public double totalSizeMb(Map<String, byte[]> hdbBytes) {
        long totalBytes = hdbBytes.values().stream().mapToLong(b -> b.length).sum();
        return totalBytes / (1024.0 * 1024.0);
    }
}
