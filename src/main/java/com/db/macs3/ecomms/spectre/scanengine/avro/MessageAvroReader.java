package com.db.macs3.ecomms.spectre.scanengine.avro;

import com.db.macs3.ecomms.spectre.scanengine.gcs.GcsClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.List;

/**
 * Reads AVRO message files for one dataset from its {@code restricted/} and
 * {@code unrestricted/} GCS subfolders (requirement 1.c, 8.d — one job run
 * reads both), filters to only {@code message_id}s the view actually
 * referenced (requirement 1.e), and tags each row with which subfolder it
 * came from plus its {@code datasetId} — the two pieces of context
 * {@link com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage}
 * needs that are not present in the AVRO itself.
 *
 * <p>Stays fully distributed: reading is Spark's own {@code avro} format
 * reader (parallelised across the underlying files automatically), and the
 * {@code message_id} filter is a broadcast-join-friendly {@code isin}/
 * semi-join against the (small, driver-collected) set of relevant ids or —
 * for a very large id set — a proper Dataset-to-Dataset join, never a
 * driver-side per-message filter.
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox — see {@code GcsClient} class Javadoc.
 */
public final class MessageAvroReader {

    private MessageAvroReader() {}

    /**
     * @param baseBucket           the resolved live/test message bucket (see
     *                              {@code ScanEngineProperties#resolveMessageBucket})
     * @param datasetId              which {@code coreapp-trans/<dataset_id>/} folder to read
     * @param relevantMessageIds     restrict to these ids only — the view's own
     *                              {@code message_id} set (requirement 1.e's "process only
     *                              those messages... derived from
     *                              spectre-audit.language-feature-decision")
     * @throws NoAvroFilesFoundException if neither the {@code restricted/} nor
     *          {@code unrestricted/} subfolder has any {@code .avro} file — requirement 3.c
     */
    public static Dataset<Row> readDataset(SparkSession spark, GcsClient gcsClient, String baseBucket,
                                            String datasetId, Dataset<Row> relevantMessageIds) {
        String restrictedPath = "gs://" + baseBucket + "/coreapp-trans/" + datasetId + "/restricted/";
        String unrestrictedPath = "gs://" + baseBucket + "/coreapp-trans/" + datasetId + "/unrestricted/";

        boolean hasRestricted = !gcsClient.listAllObjects(baseBucket,
                "coreapp-trans/" + datasetId + "/restricted/").isEmpty();
        boolean hasUnrestricted = !gcsClient.listAllObjects(baseBucket,
                "coreapp-trans/" + datasetId + "/unrestricted/").isEmpty();

        if (!hasRestricted && !hasUnrestricted) {
            throw new NoAvroFilesFoundException(
                    "No AVRO files found for dataset_id='" + datasetId + "' under either " + restrictedPath
                    + " or " + unrestrictedPath);
        }

        Dataset<Row> combined = null;
        if (hasRestricted) {
            combined = readAndTag(spark, restrictedPath, datasetId, true);
        }
        if (hasUnrestricted) {
            Dataset<Row> unrestrictedDf = readAndTag(spark, unrestrictedPath, datasetId, false);
            combined = (combined == null) ? unrestrictedDf : combined.unionByName(unrestrictedDf);
        }

        return combined.join(functions.broadcast(relevantMessageIds.dropDuplicates("message_id")), "message_id");
    }

    private static Dataset<Row> readAndTag(SparkSession spark, String path, String datasetId, boolean restricted) {
        return spark.read().format("avro").load(path)
                .withColumn("dataset_id", functions.lit(datasetId))
                .withColumn("restricted", functions.lit(restricted));
    }

    /** Thrown when a dataset has no AVRO files in either subfolder — see requirement 3.c. */
    public static final class NoAvroFilesFoundException extends RuntimeException {
        public NoAvroFilesFoundException(String message) {
            super(message);
        }
    }
}
