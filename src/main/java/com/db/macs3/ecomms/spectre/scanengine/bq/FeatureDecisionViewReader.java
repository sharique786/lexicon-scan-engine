package com.db.macs3.ecomms.spectre.scanengine.bq;

import com.db.macs3.ecomms.spectre.scanengine.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.List;

/**
 * Reads {@code vw_src_msg_lexicon_decision_mapping}, filtered by
 * {@code dataset_partition_value}, {@code feature_partition_value}, and
 * {@code process_id} (requirement 1.c), then aggregates it from one-row-
 * per-(message, feature) into one-row-per-message with a {@code features}
 * array column — the shape {@code PartitionProcessor} needs, since a
 * message's full feature set must be visible together to run
 * {@code FeatureGroupingService}/{@code DecisionTreeEvaluator} at all.
 *
 * <p>The filter is applied via the Spark DataFrame API's column-equality
 * methods (not raw SQL string concatenation), so there is no SQL-injection
 * surface even though {@code datasetPartitionValue}/{@code featurePartitionValue}/
 * {@code processId} ultimately originate from the Airflow-supplied runtime
 * parameters.
 *
 * <p>Stays fully distributed — reading and filtering happen via the Spark
 * BigQuery connector's normal predicate/column pushdown, and the subsequent
 * {@code groupBy}/{@code collect_list} aggregation is an ordinary distributed
 * shuffle. Nothing here calls {@code .collect()} or otherwise pulls
 * per-message-scale data back to the driver.
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox — see {@code GcsClient} class Javadoc.
 */
public final class FeatureDecisionViewReader {

    private FeatureDecisionViewReader() {}

    /**
     * Reads and filters the view for ONE {@code dataset_partition_value} —
     * called once per entry of {@code RuntimeArgs.datasetDetails} by the
     * caller, which unions the results across entries (requirement 8.c:
     * {@code policy-alert-test} can have several).
     */
    public static Dataset<Row> readFiltered(SparkSession spark, BqTableConfig tableConfig,
                                             String datasetPartitionValue, String featurePartitionValue,
                                             String processId) {
        Dataset<Row> raw = spark.read()
                .format("bigquery")
                .option("table", tableConfig.fullyQualifiedViewName())
                .load();

        return raw.filter(
                functions.col(BqColumns.View.DATASET_PARTITION).equalTo(datasetPartitionValue)
                        .and(functions.col(BqColumns.View.FEATURE_PARTITION_VALUE).equalTo(featurePartitionValue))
                        .and(functions.col(BqColumns.View.PROCESS_ID).equalTo(processId)));
    }

    /** Unions several dataset partitions' filtered view results into one Dataset — see {@link #readFiltered}. */
    public static Dataset<Row> unionAll(SparkSession spark, List<Dataset<Row>> perDatasetResults) {
        if (perDatasetResults.isEmpty()) {
            throw new IllegalArgumentException("perDatasetResults must not be empty");
        }
        Dataset<Row> result = perDatasetResults.get(0);
        for (int i = 1; i < perDatasetResults.size(); i++) {
            result = result.unionByName(perDatasetResults.get(i));
        }
        return result;
    }

    /**
     * Aggregates one-row-per-(message, feature) into one-row-per-message
     * with a {@code features} array-of-struct column — every original
     * column preserved inside each array element, so
     * {@code ViewRowConverter} can read them by the same names it always
     * does.
     */
    public static Dataset<Row> groupByMessageId(Dataset<Row> viewRows) {
        return viewRows.groupBy(BqColumns.View.MESSAGE_ID)
                .agg(functions.collect_list(functions.struct(viewRows.col("*"))).alias("features"));
    }
}
