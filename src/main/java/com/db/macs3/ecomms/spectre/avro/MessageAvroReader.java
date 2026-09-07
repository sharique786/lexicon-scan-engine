package com.db.macs3.ecomms.spectre.avro;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Reads AVRO message files for one dataset from its {@code restricted/} and
 * {@code unrestricted/} GCS subfolders, filters to only the {@code message_id}s
 * the view actually referenced, and tags each row with which subfolder it
 * came from plus its {@code datasetPartitionValue} (the Airflow-supplied
 * {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for this dataset,
 * not anything read from the AVRO itself) — context
 * {@link com.db.macs3.ecomms.spectre.model.message.ScanMessage}
 * needs that is not present in the AVRO itself, and the source of the
 * {@code dataset_partition_value} column all 4 per-message output tables now
 * carry (see {@code OutputRowBuilder}/{@code OutputTableWriter}).
 *
 * <p>Stays fully distributed: reading is Spark's own {@code avro} format
 * reader (parallelised across the underlying files automatically), and the
 * {@code message_id} filter is a broadcast join against the (driver-collected)
 * set of relevant ids.
 */
public final class MessageAvroReader {

    private MessageAvroReader() {}

    /**
     * @param baseBucket              {@code DataprocConfig.messages().msgGcsBucket()}
     * @param datasetPathPrefix        {@code DataprocConfig.messages().msgGcsPrefix()} — e.g.
     *                                 {@code "coreapp-trans"}, without a trailing slash
     * @param datasetId                 which {@code <datasetPathPrefix>/<dataset_id>/} folder to read —
     *                                 GCS path only; not itself tagged onto the resulting rows
     * @param datasetPartitionValue     {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for
     *                                 this same dataset — tagged onto every row read here, verbatim
     * @param relevantMessageIds        restrict to these ids only — the view's own
     *                                 {@code message_id} set
     * @throws NoAvroFilesFoundException if neither the {@code restricted/} nor
     *          {@code unrestricted/} subfolder has any {@code .avro} file
     */
    public static Dataset<Row> readDataset(SparkSession spark, GcsClient gcsClient, String baseBucket,
                                            String datasetPathPrefix, String datasetId, String datasetPartitionValue,
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
            combinedMessages = readAndTag(spark, restrictedPath, datasetPartitionValue, true);
        }
        if (hasUnrestrictedFiles) {
            Dataset<Row> unrestrictedMessages = readAndTag(spark, unrestrictedPath, datasetPartitionValue, false);
            combinedMessages = (combinedMessages == null)
                    ? unrestrictedMessages
                    : combinedMessages.unionByName(unrestrictedMessages);
        }

        return combinedMessages.join(
                functions.broadcast(relevantMessageIds.dropDuplicates(BqColumns.View.MESSAGE_ID)),
                BqColumns.View.MESSAGE_ID);
    }

    private static Dataset<Row> readAndTag(SparkSession spark, String path, String datasetPartitionValue, boolean restricted) {
        return spark.read()
                .format(AvroConstants.FORMAT)
                .option("pathGlobFilter", "*.avro")
                .load(path)
                .withColumn(AvroConstants.COLUMN_DATASET_PARTITION_VALUE, functions.lit(datasetPartitionValue))
                .withColumn(AvroConstants.COLUMN_RESTRICTED, functions.lit(restricted));
    }

    /** Thrown when a dataset has no AVRO files in either subfolder. */
    public static final class NoAvroFilesFoundException extends RuntimeException {
        public NoAvroFilesFoundException(String message) {
            super(message);
        }
    }
}
