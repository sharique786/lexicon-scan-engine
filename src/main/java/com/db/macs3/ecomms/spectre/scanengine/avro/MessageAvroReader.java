package com.db.macs3.ecomms.spectre.scanengine.avro;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.gcs.GcsClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Reads AVRO message files for one dataset from its {@code restricted/} and
 * {@code unrestricted/} GCS subfolders, filters to only the {@code message_id}s
 * the view actually referenced, and tags each row with which subfolder it
 * came from plus its {@code datasetId} — context
 * {@link com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage}
 * needs that is not present in the AVRO itself.
 *
 * <p>Stays fully distributed: reading is Spark's own {@code avro} format
 * reader (parallelised across the underlying files automatically), and the
 * {@code message_id} filter is a broadcast join against the (driver-collected)
 * set of relevant ids.
 */
public final class MessageAvroReader {

    private MessageAvroReader() {}

    /**
     * @param baseBucket           {@code DataprocConfig.messages().msgGcsBucket()}
     * @param datasetPathPrefix     {@code DataprocConfig.messages().msgGcsPrefix()} — e.g.
     *                              {@code "coreapp-trans"}, without a trailing slash
     * @param datasetId              which {@code <datasetPathPrefix>/<dataset_id>/} folder to read
     * @param relevantMessageIds     restrict to these ids only — the view's own
     *                              {@code message_id} set
     * @throws NoAvroFilesFoundException if neither the {@code restricted/} nor
     *          {@code unrestricted/} subfolder has any {@code .avro} file
     */
    public static Dataset<Row> readDataset(SparkSession spark, GcsClient gcsClient, String baseBucket,
                                            String datasetPathPrefix, String datasetId,
                                            Dataset<Row> relevantMessageIds) {
        String datasetPrefix = datasetPathPrefix + "/" + datasetId + "/";
        String restrictedPrefix = datasetPrefix + AvroConstants.RESTRICTED_SUBFOLDER;
        String unrestrictedPrefix = datasetPrefix + AvroConstants.UNRESTRICTED_SUBFOLDER;
        String restrictedPath = "gs://" + baseBucket + "/" + restrictedPrefix;
        String unrestrictedPath = "gs://" + baseBucket + "/" + unrestrictedPrefix;

        boolean hasRestrictedFiles = !gcsClient.listAllObjects(baseBucket, restrictedPrefix).isEmpty();
        boolean hasUnrestrictedFiles = !gcsClient.listAllObjects(baseBucket, unrestrictedPrefix).isEmpty();

        if (!hasRestrictedFiles && !hasUnrestrictedFiles) {
            throw new NoAvroFilesFoundException(
                    "No AVRO files found for dataset_id='" + datasetId + "' under either " + restrictedPath
                    + " or " + unrestrictedPath);
        }

        Dataset<Row> combinedMessages = null;
        if (hasRestrictedFiles) {
            combinedMessages = readAndTag(spark, restrictedPath, datasetId, true);
        }
        if (hasUnrestrictedFiles) {
            Dataset<Row> unrestrictedMessages = readAndTag(spark, unrestrictedPath, datasetId, false);
            combinedMessages = (combinedMessages == null)
                    ? unrestrictedMessages
                    : combinedMessages.unionByName(unrestrictedMessages);
        }

        return combinedMessages.join(
                functions.broadcast(relevantMessageIds.dropDuplicates(BqColumns.View.MESSAGE_ID)),
                BqColumns.View.MESSAGE_ID);
    }

    private static Dataset<Row> readAndTag(SparkSession spark, String path, String datasetId, boolean restricted) {
        return spark.read().format(AvroConstants.FORMAT).load(path)
                .withColumn(AvroConstants.COLUMN_DATASET_ID, functions.lit(datasetId))
                .withColumn(AvroConstants.COLUMN_RESTRICTED, functions.lit(restricted));
    }

    /** Thrown when a dataset has no AVRO files in either subfolder. */
    public static final class NoAvroFilesFoundException extends RuntimeException {
        public NoAvroFilesFoundException(String message) {
            super(message);
        }
    }
}
