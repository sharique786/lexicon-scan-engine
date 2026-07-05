package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.JobConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Loads the {@link JobConfig} JSON configuration file from a GCS path.
 *
 * <p>Per the platform's operating convention, BigQuery/view table names and
 * Spark tuning are not passed as individual CLI flags on every Dataproc job
 * submission — they live in a versioned JSON file on GCS, referenced by the
 * {@code --configGcsPath} runtime argument. This keeps DAG definitions stable
 * across environments (dev/test/prod) since only the config file path
 * changes, not the DAG's task definitions.
 */
@Component
public class JobConfigReader {

    private static final Logger log = LoggerFactory.getLogger(JobConfigReader.class);

    private final Storage storageClient;
    private final ObjectMapper objectMapper;

    public JobConfigReader() {
        this.storageClient = StorageOptions.getDefaultInstance().getService();
        this.objectMapper  = new ObjectMapper();
    }

    /** Package-private constructor for unit testing with a mock Storage client. */
    JobConfigReader(Storage storageClient, ObjectMapper objectMapper) {
        this.storageClient = storageClient;
        this.objectMapper  = objectMapper;
    }

    /**
     * Loads and parses the {@link JobConfig} from the given {@code gs://} path.
     *
     * @param configGcsPath full GCS URI, e.g. {@code gs://bucket/config/scan-engine.json}
     * @return the parsed job configuration
     * @throws IOException if the file cannot be read or parsed
     */
    public JobConfig load(String configGcsPath) throws IOException {
        log.info("Loading job config from: {}", configGcsPath);
        GcsPath path = GcsPath.parse(configGcsPath);

        Blob blob = storageClient.get(BlobId.of(path.bucket, path.object));
        if (blob == null || !blob.exists()) {
            throw new IOException("Job config file not found at: " + configGcsPath);
        }

        byte[] bytes = blob.getContent();
        String json  = new String(bytes, StandardCharsets.UTF_8);
        JobConfig config = objectMapper.readValue(json, JobConfig.class);

        log.info("Job config loaded: bqProject={}, bqDataset={}, viewName={}",
                 config.getBqProject(), config.getBqDataset(), config.getViewName());
        return config;
    }

    /** Minimal {@code gs://bucket/object/path} URI parser (no external dependency needed). */
    static class GcsPath {
        final String bucket;
        final String object;

        private GcsPath(String bucket, String object) {
            this.bucket = bucket;
            this.object = object;
        }

        static GcsPath parse(String gsUri) {
            if (!gsUri.startsWith("gs://")) {
                throw new IllegalArgumentException("Not a valid gs:// URI: " + gsUri);
            }
            String withoutScheme = gsUri.substring("gs://".length());
            int firstSlash = withoutScheme.indexOf('/');
            if (firstSlash < 0) {
                throw new IllegalArgumentException("gs:// URI missing object path: " + gsUri);
            }
            String bucket = withoutScheme.substring(0, firstSlash);
            String object = withoutScheme.substring(firstSlash + 1);
            return new GcsPath(bucket, object);
        }
    }
}
