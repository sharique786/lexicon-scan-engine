package com.db.macs3.ecomms.spectre.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The feature-set sort expression used by ScanEngineJobRunner must be valid Spark SQL and group equal sets. */
class PartitionSortExpressionTest {

    @Test
    void sortExpression_groupsMessagesWithTheSameFeatureSet() {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("sort-expr-test")
                .config("spark.ui.enabled", "false").getOrCreate();
        Dataset<Row> df = spark.sql(
                "select * from values "
                + "(1, array(named_struct('feature_definition','A'), named_struct('feature_definition','B'))),"
                + "(2, array(named_struct('feature_definition','C'))),"
                + "(3, array(named_struct('feature_definition','B'), named_struct('feature_definition','A'))),"
                + "(4, array(named_struct('feature_definition','C'))) as t(id, features)");
        List<Row> rows = df.repartition(1).sortWithinPartitions(functions.expr(
                "hash(sort_array(transform(features, f -> f.feature_definition)))")).collectAsList();

        // equal feature sets (1&3, 2&4) end up adjacent regardless of element order
        int posOf1 = indexOf(rows, 1), posOf3 = indexOf(rows, 3);
        int posOf2 = indexOf(rows, 2), posOf4 = indexOf(rows, 4);
        assertEquals(1, Math.abs(posOf1 - posOf3));
        assertEquals(1, Math.abs(posOf2 - posOf4));
    }

    private static int indexOf(List<Row> rows, int id) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).getInt(0) == id) {
                return i;
            }
        }
        return -1;
    }
}
