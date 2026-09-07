package com.db.macs3.ecomms.spectre.spark;

/**
 * Column names of the joined (message + view) {@code Row} that
 * {@link ScanEngineJobRunner} builds and {@link PartitionProcessor} reads.
 * {@link #DATASET_PARTITION_VALUE} is not a BigQuery/AVRO source column
 * either — it's tagged onto every message row by
 * {@code MessageAvroReader.readAndTag} from {@code RuntimeArgs.DatasetDetail
 * #datasetPartitionValue()}, the same way {@link #FEATURES}/{@link #RESTRICTED}
 * are added by {@code ScanEngineJobRunner.runPipeline}'s {@code withColumn} calls.
 */
final class JoinedRowColumns {

    private JoinedRowColumns() {}

    static final String FEATURES = "features";
    static final String RESTRICTED = "restricted";
    /** Tagged by {@code MessageAvroReader} — see class Javadoc. */
    static final String DATASET_PARTITION_VALUE = "dataset_partition_value";
    static final String PIPELINE_EXEC_ID_FOR_OUTPUT = "pipeline_exec_id_for_output";
    static final String CREATED_BY_FOR_OUTPUT = "created_by_for_output";
    static final String DATASET_PARTITION_VALUE_FOR_OUTPUT = "dataset_partition_value_for_output";
}
