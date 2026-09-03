package com.db.macs3.ecomms.spectre.scanengine.spark;

/** Spark runtime/static config keys this job sets explicitly — see {@link ScanEngineJobRunner}. */
final class SparkConfigKeys {

    private SparkConfigKeys() {}

    static final String SERIALIZER = "spark.serializer";
    static final String ADAPTIVE_ENABLED = "spark.sql.adaptive.enabled";
    static final String ADAPTIVE_COALESCE_PARTITIONS_ENABLED = "spark.sql.adaptive.coalescePartitions.enabled";
    static final String ADAPTIVE_SKEW_JOIN_ENABLED = "spark.sql.adaptive.skewJoin.enabled";
    static final String ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_FACTOR = "spark.sql.adaptive.skewJoin.skewedPartitionFactor";
    static final String ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_THRESHOLD_BYTES =
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes";
    static final String ADAPTIVE_ADVISORY_PARTITION_SIZE_BYTES = "spark.sql.adaptive.advisoryPartitionSizeInBytes";
    static final String SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    static final String FILES_MAX_PARTITION_BYTES = "spark.sql.files.maxPartitionBytes";
}
