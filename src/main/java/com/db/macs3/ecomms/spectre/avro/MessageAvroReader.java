package com.db.macs3.ecomms.spectre.avro;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Reads the AVRO message files of one dataset from its {@code restricted/} and {@code unrestricted/} GCS subfolders
 * ({@code gs://<msg-gcs-bucket>/<msg-gcs-prefix>/<dataset_id>/{restricted,unrestricted}/}), keeps only the
 * {@code message_id}s the view referenced, and tags each row with:
 * <ul>
 *   <li>{@code restricted} — which subfolder it was read from (decides the {@code lexicon-hit-restricted} vs
 *       {@code -unrestricted} table), and</li>
 *   <li>{@code dataset_partition_value} — the value {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} gives
 *       for this dataset, which becomes the output tables' {@code dataset_partition_value} column.</li>
 * </ul>
 * Neither tag is present in the AVRO itself.
 *
 * <p>Stays fully distributed: Spark's own {@code avro} reader parallelises across the files, and the
 * {@code message_id} filter is a broadcast join against the view's distinct {@code message_id} {@code Dataset}
 * (never collected to the driver).
 */
public final class MessageAvroReader {

    private static final Logger log = LoggerFactory.getLogger(MessageAvroReader.class);

    private MessageAvroReader() {
    }

    /**
     * @param baseBucket            {@code DataprocConfig.messages().msgGcsBucket()}
     * @param datasetPathPrefix     {@code DataprocConfig.messages().msgGcsPrefix()} — e.g.
     *                              {@code "coreapp-trans"}, without a trailing slash
     * @param datasetId             which {@code <datasetPathPrefix>/<dataset_id>/} folder to read —
     *                              GCS path only; not itself tagged onto the resulting rows
     * @param datasetPartitionValue {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for
     *                              this same dataset — tagged onto every row read here, verbatim
     * @param relevantMessageIds    restrict to these ids only — the view's own
     *                              {@code message_id} set
     * @throws NoAvroFilesFoundException if neither the {@code restricted/} nor
     *                                   {@code unrestricted/} subfolder has any {@code .avro} file
     */
    public static Dataset<Row> readDataset(SparkSession spark, GcsClient gcsClient, String baseBucket,
                                           String datasetPathPrefix, String datasetId, String datasetPartitionValue,
                                           Dataset<Row> relevantMessageIds) {
        String datasetPrefix = datasetPathPrefix + "/" + datasetId + "/";
        String restrictedPrefix = datasetPrefix + AvroConstants.RESTRICTED_SUBFOLDER;
        String unrestrictedPrefix = datasetPrefix + AvroConstants.UNRESTRICTED_SUBFOLDER;
        String restrictedPath = restrictedPath(baseBucket, datasetPathPrefix, datasetId);
        String unrestrictedPath = "gs://" + baseBucket + "/" + unrestrictedPrefix;
        log.info("Reading AVRO messages for dataset_id='{}': restrictedPath={}, unrestrictedPath={}, "
                        + "datasetPartitionValue={}",
                datasetId, restrictedPath, unrestrictedPath, datasetPartitionValue);

        boolean hasRestrictedFiles = hasAvroFile(gcsClient.listAllObjects(baseBucket, restrictedPrefix));
        boolean hasUnrestrictedFiles = hasAvroFile(gcsClient.listAllObjects(baseBucket, unrestrictedPrefix));
        log.info("dataset_id='{}': hasRestrictedAvroFiles={}, hasUnrestrictedAvroFiles={}",
                datasetId, hasRestrictedFiles, hasUnrestrictedFiles);

        if (!hasRestrictedFiles && !hasUnrestrictedFiles) {
            throw new NoAvroFilesFoundException(
                    "No AVRO files found for dataset_id='" + datasetId + "' under either " + restrictedPath
                            + " or " + unrestrictedPath);
        }

        Dataset<Row> combinedMessages = null;
        if (hasRestrictedFiles) {
            log.info("Loading restricted AVRO messages for dataset_id='{}' from {}", datasetId, restrictedPath);
            combinedMessages = readAndTag(spark, restrictedPath, datasetPartitionValue, true);
        }
        if (hasUnrestrictedFiles) {
            log.info("Loading unrestricted AVRO messages for dataset_id='{}' from {}", datasetId, unrestrictedPath);
            Dataset<Row> unrestrictedMessages = readAndTag(spark, unrestrictedPath, datasetPartitionValue, false);
            combinedMessages = (combinedMessages == null)
                    ? unrestrictedMessages
                    : combinedMessages.unionByName(unrestrictedMessages);
        }

        if (combinedMessages == null) {
            throw new IllegalStateException("MessageAvroReader: combinedMessages in null after reading AVRO files");
        }

        return combinedMessages.join(
                functions.broadcast(relevantMessageIds.dropDuplicates(BqColumns.View.MESSAGE_ID)),
                BqColumns.View.MESSAGE_ID);
    }

    /**
     * The {@code restricted/} AVRO subfolder path for one dataset — the same path {@link #readDataset}
     * reads from. Exposed so other callers needing a path colocated with the restricted messages
     * (e.g. {@code ScanEngineJobRunner}'s restricted CSV mirror) derive it from this one place
     * rather than re-deriving the folder convention themselves.
     *
     * @param baseBucket        {@code DataprocConfig.messages().msgGcsBucket()}
     * @param datasetPathPrefix {@code DataprocConfig.messages().msgGcsPrefix()}
     * @param datasetId         which {@code <datasetPathPrefix>/<dataset_id>/} folder
     */
    public static String restrictedPath(String baseBucket, String datasetPathPrefix, String datasetId) {
        return "gs://" + baseBucket + "/" + datasetPathPrefix + "/" + datasetId + "/" + AvroConstants.RESTRICTED_SUBFOLDER;
    }

    /**
     * {@code gcsClient.listAllObjects} returns every object under the prefix, not just
     * {@code .avro} files — the {@code restricted/} prefix in particular also holds this job's
     * own CSV mirror output ({@code ScanEngineJobRunner#writeRestrictedCsvMirror} writes to
     * {@code restrictedPath + "csv"}, i.e. {@code restricted/csv/part-*.csv} + {@code _SUCCESS}
     * left behind by a prior run of this same dataset). Without this filter, a rerun with only
     * {@code unrestricted/} AVRO files present would still see {@code restricted/}'s leftover CSV
     * objects and wrongly conclude AVRO files exist there, leading {@link #readAndTag} to load a
     * path with zero files actually matching {@code pathGlobFilter=*.avro} — which Spark's own
     * AVRO datasource reports as {@code FileNotFoundException: No Avro files found}.
     */
    private static boolean hasAvroFile(List<String> objectNames) {
        return objectNames.stream().anyMatch(name -> name.endsWith(".avro"));
    }

    private static Dataset<Row> readAndTag(SparkSession spark, String path, String datasetPartitionValue, boolean restricted) {
        return spark.read()
                .format(AvroConstants.FORMAT)
                .option("pathGlobFilter", "*.avro")
                .load(path)
                .withColumn(AvroConstants.COLUMN_DATASET_PARTITION_VALUE, functions.lit(datasetPartitionValue))
                .withColumn(AvroConstants.COLUMN_RESTRICTED, functions.lit(restricted));
    }

    /**
     * Thrown when a dataset has no AVRO files in either subfolder.
     */
    public static final class NoAvroFilesFoundException extends RuntimeException {
        public NoAvroFilesFoundException(String message) {
            super(message);
        }
    }
}
