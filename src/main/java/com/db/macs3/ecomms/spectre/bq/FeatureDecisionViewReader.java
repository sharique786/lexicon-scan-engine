package com.db.macs3.ecomms.spectre.bq;

import com.db.macs3.ecomms.spectre.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Reads {@code vw_src_msg_lexicon_decision_mapping}, filtered by
 * {@code process_id}, {@code feature_partition_value}, {@code policy_engine_id},
 * and every {@code dataset_partition_value} in {@code runtimeArgs.datasetDetails()}
 * (via one {@code dataset_partition IN (...)} clause — a single query covers
 * every dataset detail entry, rather than one query per entry unioned
 * together), then aggregates it from one-row-per-(message, feature) into
 * one-row-per-message with a {@code features} array column — the shape
 * {@code PartitionProcessor} needs, since a message's full feature set must
 * be visible together to run
 * {@code FeatureGroupingService}/{@code DecisionTreeEvaluator} at all.
 *
 * <p>The filter is pushed down to BigQuery as a raw SQL predicate string via
 * the connector's {@code "filter"} option. The values composed into it
 * originate from the Airflow-supplied runtime parameters, not untrusted
 * user input.
 *
 * <p>Stays fully distributed — reading and filtering happen via the Spark
 * BigQuery connector's normal predicate/column pushdown, and the subsequent
 * {@code groupBy}/{@code collect_list} aggregation is an ordinary distributed
 * shuffle. Nothing here calls {@code .collect()} or otherwise pulls
 * per-message-scale data back to the driver.
 */
public final class FeatureDecisionViewReader {

    private static final Logger log = LoggerFactory.getLogger(FeatureDecisionViewReader.class);

    /** Alias for the aggregated per-message array-of-struct column {@link #groupByMessageId} produces. */
    private static final String FEATURES_COLUMN = "features";

    private FeatureDecisionViewReader() {}

    /**
     * Reads and filters the view in ONE query covering every entry of
     * {@code runtimeArgs.datasetDetails()} at once, via a {@code dataset_partition
     * IN (...)} predicate — pushed down to BigQuery as a raw SQL filter string
     * (the connector's {@code "filter"} option), rather than issuing one query
     * per dataset and unioning the results.
     */
    public static Dataset<Row> readFiltered(SparkSession spark, BqTableConfig tableConfig,
                                             RuntimeArgs runtimeArgs) {

        String datasetPartitionIn = runtimeArgs.datasetDetails().stream()
                .map(dv -> "'" + dv.datasetPartitionValue() + "'")
                .collect(Collectors.joining(", "));

        String filter = "process_id = '" + runtimeArgs.processId() + "'"
                + " AND feature_partition_value = '" + runtimeArgs.featurePartitionValue() + "'"
                + " AND dataset_partition IN (" + datasetPartitionIn + ")"
                + " AND policy_engine_id = " + runtimeArgs.policyEngineId();

        Dataset<Row> raw = spark.read()
                .format("bigquery")
                .option("project", tableConfig.bqProject())
                .option("table", tableConfig.fullyQualifiedViewName())
                .option("viewsEnabled", "true")
                .option("materializationProject", tableConfig.bqProject())
                .option("materializationDataset", tableConfig.bqDataset())
                .option("filter", filter)
                .load();

        log.info("Read {} rows from {}.{} with filter: {}", raw.count(), tableConfig.bqDataset(), tableConfig.bqViewName(), filter);
        return raw;
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
                .agg(functions.collect_list(functions.struct(viewRows.col("*"))).alias(FEATURES_COLUMN));
    }
}
