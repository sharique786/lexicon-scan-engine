package com.db.macs3.ecomms.spectre.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: builds a REAL local-mode {@link SparkSession} via
 * {@link SparkSessionConfig#sparkSession()} — not a mock — to confirm the
 * job-specific runtime config previously inlined in
 * {@code ScanEngineJobRunner.applyJobSpecificSparkConf} (see that class's
 * git history) still gets applied now that it lives here. {@code spark.master}
 * is supplied via a system property rather than a builder call, since
 * {@link SparkSessionConfig} itself deliberately never sets a master (a real
 * Dataproc run gets one from {@code spark-submit}) — {@code SparkConf}'s
 * default constructor picks up any {@code spark.*} system property, so this
 * does not require touching production code to make it testable.
 */
@DisplayName("SparkSessionConfig")
class SparkSessionConfigTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        System.setProperty("spark.master", "local[2]");
        System.setProperty("spark.ui.enabled", "false");
        spark = new SparkSessionConfig().sparkSession();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        System.clearProperty("spark.master");
        System.clearProperty("spark.ui.enabled");
    }

    @Test
    @DisplayName("sets spark.serializer to KryoSerializer (a static config, so it must be set on the builder)")
    void setsKryoSerializer() {
        assertThat(spark.sparkContext().getConf().get("spark.serializer"))
                .isEqualTo("org.apache.spark.serializer.KryoSerializer");
    }

    @Test
    @DisplayName("enables AQE, coalescePartitions, and skewJoin — never left to the shared cluster's defaults")
    void enablesAdaptiveQueryExecution() {
        assertThat(spark.conf().get("spark.sql.adaptive.enabled")).isEqualTo("true");
        assertThat(spark.conf().get("spark.sql.adaptive.coalescePartitions.enabled")).isEqualTo("true");
        assertThat(spark.conf().get("spark.sql.adaptive.skewJoin.enabled")).isEqualTo("true");
    }

    @Test
    @DisplayName("tightens the skew-join partition factor/threshold from Spark's own defaults")
    void tightensSkewJoinThresholds() {
        assertThat(spark.conf().get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")).isEqualTo("3");
        assertThat(spark.conf().get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")).isEqualTo("128m");
        assertThat(spark.conf().get("spark.sql.adaptive.advisoryPartitionSizeInBytes")).isEqualTo("64m");
    }

    @Test
    @DisplayName("floors shuffle.partitions at 200 even under local[2]'s low default parallelism")
    void floorsShufflePartitionsAt200() {
        // defaultParallelism (2 under local[2]) * 3 = 6, well under the 200 floor — this
        // asserts the floor branch, not the "high-parallelism cluster" branch.
        assertThat(spark.conf().get("spark.sql.shuffle.partitions")).isEqualTo("200");
    }

    @Test
    @DisplayName("caps AVRO read-side partitions at 64MB")
    void capsMaxPartitionBytes() {
        assertThat(spark.conf().get("spark.sql.files.maxPartitionBytes")).isEqualTo("67108864");
    }

    @Test
    @DisplayName("javaSparkContext() wraps the SAME underlying SparkContext as sparkSession(), not a second one")
    void javaSparkContextWrapsSameSparkContext() {
        JavaSparkContext javaSparkContext = new SparkSessionConfig().javaSparkContext(spark);

        assertThat(javaSparkContext.sc()).isSameAs(spark.sparkContext());
    }
}
