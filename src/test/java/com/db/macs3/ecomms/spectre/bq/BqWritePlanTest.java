package com.db.macs3.ecomms.spectre.bq;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("BqWritePlan + OutputTableWriter.batchFilter")
class BqWritePlanTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]").appName("bq-write-plan-test")
                .config("spark.ui.enabled", "false").getOrCreate();
    }

    @Test
    @DisplayName("no rows -> empty plan (write skipped)")
    void empty() {
        assertThat(BqWritePlan.forRows(0, 1000, 0).isEmpty()).isTrue();
    }

    @Test
    @DisplayName("partitions = ceil(rows / rowsPerPartition); one load job when splitting is disabled")
    void partitions() {
        BqWritePlan plan = BqWritePlan.forRows(248_054, 50_000, 0);

        assertThat(plan.partitions()).isEqualTo(5);
        assertThat(plan.loadJobs()).isEqualTo(1);
        assertThat(BqWritePlan.forRows(3, 20_000, 0).partitions()).isEqualTo(1);
    }

    @Test
    @DisplayName("load jobs = ceil(rows / maxRowsPerLoadJob) when enabled")
    void loadJobs() {
        assertThat(BqWritePlan.forRows(10_000_000, 50_000, 4_000_000).loadJobs()).isEqualTo(3);
        assertThat(BqWritePlan.forRows(1_000, 50_000, 4_000_000).loadJobs()).isEqualTo(1);
    }

    @Test
    @DisplayName("rejects a non-positive rows-per-partition")
    void rejectsBadTarget() {
        assertThatThrownBy(() -> BqWritePlan.forRows(10, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("batch filters put every row in exactly one batch")
    void batchesPartitionTheRows() {
        Dataset<Row> rows = spark.range(10_000).selectExpr("concat('msg-', id) as message_id");
        int batches = 4;
        long total = 0;
        for (int i = 0; i < batches; i++) {
            long n = rows.filter(OutputTableWriter.batchFilter("message_id", batches, i)).count();
            assertThat(n).isPositive();
            total += n;
        }
        assertThat(total).isEqualTo(10_000);
    }
}
