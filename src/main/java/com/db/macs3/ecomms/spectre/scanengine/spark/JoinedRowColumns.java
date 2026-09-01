package com.db.macs3.ecomms.spectre.scanengine.spark;

/**
 * Column names of the joined (message + view) {@code Row} that
 * {@link ScanEngineJobRunner} builds and {@link PartitionProcessor} reads.
 * These are not BigQuery/AVRO source columns — they're added by
 * {@code ScanEngineJobRunner.runPipeline}'s {@code withColumn} calls, or
 * come from the AVRO message schema's own {@code dataset_id} field.
 */
final class JoinedRowColumns {

    private JoinedRowColumns() {}

    static final String FEATURES = "features";
    static final String RESTRICTED = "restricted";
    static final String DATASET_ID = "dataset_id";
    static final String PIPELINE_EXEC_ID_FOR_OUTPUT = "pipeline_exec_id_for_output";
    static final String CREATED_BY_FOR_OUTPUT = "created_by_for_output";
    static final String DATASET_PARTITION_VALUE_FOR_OUTPUT = "dataset_partition_value_for_output";
}
