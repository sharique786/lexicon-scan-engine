package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * How view rows and joined message rows are shaped before scanning — two independent concerns:
 *
 * <h2>Short feature-definition keys (always on)</h2>
 * The view repeats the full {@code feature_definition} JSON on every (message, feature) row. Grouping by
 * message ({@code collect_list(struct(*))}) and joining with the messages would shuffle that JSON once per row,
 * and every row deserialised on an executor would carry its own new {@code String} copy, re-hashed by every
 * {@code FeatureDefinition.parseCached} lookup. Instead each DISTINCT definition gets a small INT
 * ({@link #assignKeys}); {@link #withDefinitionKeys} swaps the JSON column for that key before the shuffles,
 * and executors resolve it through a broadcast key -> JSON map ({@code ViewRowConverter.fromRow(row, map)}).
 * Every row of one definition then shares ONE {@code String} instance per executor, whose hash is cached and
 * whose {@code equals} short-circuits on identity.
 *
 * <h2>Grouping messages by feature set ({@link #layout})</h2>
 * The join leaves messages hash-partitioned by {@code message_id}, so every partition — and every executor — sees
 * every feature set and loads every bundle. {@code sortWithinPartitions} (default on) only reorders rows inside a
 * partition. {@code partitionByFeatureSet} (opt-in) additionally RANGE-partitions by (feature-set hash,
 * {@code message_id}): messages with the same feature set land in the same or neighbouring partitions, so each
 * partition/executor touches far fewer bundles. {@code message_id} is the tie-breaker, so one very common feature
 * set is split across several partitions instead of all landing in one (plain hash partitioning by feature set
 * would do that — a severe skew). Cost: one extra shuffle of the joined rows, whole messages and attachments
 * included, plus Spark's sampling pass for the range bounds — measure before enabling.
 */
final class FeatureSetLayout {

    private FeatureSetLayout() {
    }

    /** Order-independent hash of a joined row's feature set (equal sets -> equal hash). */
    static final String FEATURE_SET_HASH_SQL = "hash(sort_array(transform(" + JoinedRowColumns.FEATURES
            + ", f -> f." + BqColumns.View.FEATURE_DEFINITION_KEY + ")))";

    private static final String LOOKUP_DEFINITION_COLUMN = "feature_definition_lookup";

    static Column featureSetHash() {
        return functions.expr(FEATURE_SET_HASH_SQL);
    }

    /**
     * Deterministic key per distinct, non-null definition (sorted, so a re-run assigns the same keys).
     */
    static Map<String, Integer> assignKeys(Collection<String> distinctDefinitions) {
        Map<String, Integer> keys = new LinkedHashMap<>();
        for (String definition : new TreeSet<>(distinctDefinitions.stream().filter(Objects::nonNull).toList())) {
            keys.put(definition, keys.size());
        }
        return keys;
    }

    /** The executor-side lookup: key -> definition JSON. */
    static HashMap<Integer, String> invert(Map<String, Integer> keys) {
        HashMap<Integer, String> byKey = new HashMap<>(keys.size() * 2);
        keys.forEach((definition, key) -> byKey.put(key, definition));
        return byKey;
    }

    /**
     * Replaces {@code feature_definition} with {@code feature_definition_key} — via a broadcast join against the
     * small (definition, key) table. Left join: a row whose definition has no key (null JSON) keeps a null key
     * and fails later per message, exactly as a null definition did before.
     */
    static Dataset<Row> withDefinitionKeys(SparkSession spark, Dataset<Row> viewRows, Map<String, Integer> keys) {
        List<Row> keyRows = new ArrayList<>(keys.size());
        keys.forEach((definition, key) -> keyRows.add(RowFactory.create(definition, key)));
        StructType schema = new StructType()
                .add(LOOKUP_DEFINITION_COLUMN, DataTypes.StringType, false)
                .add(BqColumns.View.FEATURE_DEFINITION_KEY, DataTypes.IntegerType, false);
        Dataset<Row> keyTable = spark.createDataFrame(keyRows, schema);

        return viewRows
                .join(functions.broadcast(keyTable),
                        viewRows.col(BqColumns.View.FEATURE_DEFINITION).equalTo(keyTable.col(LOOKUP_DEFINITION_COLUMN)),
                        "left")
                .drop(LOOKUP_DEFINITION_COLUMN)
                .drop(BqColumns.View.FEATURE_DEFINITION);
    }

    /**
     * Orders/partitions the joined rows before scanning — see class Javadoc. Neither option changes results.
     */
    static Dataset<Row> layout(Dataset<Row> joined, boolean partitionByFeatureSet, boolean sortByFeatureSet) {
        Dataset<Row> laidOut = joined;
        if (partitionByFeatureSet) {
            laidOut = laidOut.repartitionByRange(featureSetHash(), functions.col(BqColumns.View.MESSAGE_ID));
        }
        if (sortByFeatureSet) {
            laidOut = laidOut.sortWithinPartitions(featureSetHash());
        }
        return laidOut;
    }
}
