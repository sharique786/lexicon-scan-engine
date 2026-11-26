package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.ViewRowConverter;
import com.db.macs3.ecomms.spectre.bq.FeatureDecisionViewReader;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Short definition keys and the feature-set sort/range layout used by ScanEngineJobRunner. */
class FeatureSetLayoutTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]").appName("feature-set-layout-test")
                .config("spark.ui.enabled", "false").getOrCreate();
    }

    @AfterEach
    void resetConf() {
        spark.conf().unset("spark.sql.shuffle.partitions");
        spark.conf().unset("spark.sql.adaptive.enabled");
    }

    @Test
    void sortExpression_groupsMessagesWithTheSameFeatureSet() {
        Dataset<Row> df = spark.sql(
                "select * from values "
                + "(1, array(named_struct('feature_definition_key',0), named_struct('feature_definition_key',1))),"
                + "(2, array(named_struct('feature_definition_key',2))),"
                + "(3, array(named_struct('feature_definition_key',1), named_struct('feature_definition_key',0))),"
                + "(4, array(named_struct('feature_definition_key',2))) as t(message_id, features)");
        List<Row> rows = FeatureSetLayout.layout(df.repartition(1), false, true).collectAsList();

        // equal feature sets (1&3, 2&4) end up adjacent regardless of element order
        assertThat(Math.abs(indexOf(rows, 1) - indexOf(rows, 3))).isEqualTo(1);
        assertThat(Math.abs(indexOf(rows, 2) - indexOf(rows, 4))).isEqualTo(1);
    }

    @Test
    void assignKeys_isDeterministicAndSkipsNull() {
        List<String> defs = new ArrayList<>(List.of("{\"b\":1}", "{\"a\":1}", "{\"c\":1}"));
        defs.add(null);
        Map<String, Integer> keys = FeatureSetLayout.assignKeys(defs);
        assertThat(keys).containsExactly(Map.entry("{\"a\":1}", 0), Map.entry("{\"b\":1}", 1), Map.entry("{\"c\":1}", 2));
        assertThat(FeatureSetLayout.assignKeys(List.of("{\"c\":1}", "{\"a\":1}", "{\"b\":1}"))).isEqualTo(keys);
        assertThat(FeatureSetLayout.invert(keys)).containsEntry(1, "{\"b\":1}").hasSize(3);
    }

    @Test
    void withDefinitionKeys_replacesJsonWithKey_andConverterResolvesItBack() {
        Dataset<Row> viewRows = spark.sql("select * from values "
                + "('m1', 'DEF_A', 'lexA'), ('m1', 'DEF_B', 'lexB'), ('m2', 'DEF_A', 'lexA'), ('m3', null, 'lexX') "
                + "as t(message_id, feature_definition, lexicon_features_to_apply_name)");
        Map<String, Integer> keys = FeatureSetLayout.assignKeys(List.of("DEF_A", "DEF_B"));
        Map<Integer, String> byKey = FeatureSetLayout.invert(keys);

        Dataset<Row> keyed = FeatureSetLayout.withDefinitionKeys(spark, viewRows, keys);
        assertThat(keyed.columns()).doesNotContain(BqColumns.View.FEATURE_DEFINITION)
                .contains(BqColumns.View.FEATURE_DEFINITION_KEY);
        assertThat(keyed.count()).isEqualTo(4); // left join: the null-definition row is kept

        Map<String, List<String>> defsByMessage = new HashMap<>();
        for (Row grouped : FeatureDecisionViewReader.groupByMessageId(keyed).collectAsList()) {
            List<String> defs = new ArrayList<>();
            for (Row featureRow : grouped.<Row>getList(grouped.fieldIndex("features"))) {
                FeatureDecisionRow row = ViewRowConverter.fromRow(featureRow, byKey);
                defs.add(row.getFeatureDefinitionJson());
            }
            defsByMessage.put(grouped.getAs("message_id"), defs);
        }
        assertThat(defsByMessage.get("m1")).containsExactlyInAnyOrder("DEF_A", "DEF_B");
        assertThat(defsByMessage.get("m2")).containsExactly("DEF_A");
        assertThat(defsByMessage.get("m3")).containsExactly((String) null); // fails later per message, as before

        // every row of one definition resolves to the SAME String instance (cached hash, identity equals)
        Row anyKeyed = keyed.filter("message_id = 'm2'").first();
        assertThat(ViewRowConverter.fromRow(anyKeyed, byKey).getFeatureDefinitionJson()).isSameAs(byKey.get(0));
    }

    @Test
    void converter_unknownKeyFails_andJsonColumnStillReadWithoutMap() {
        Row keyedRow = spark.sql("select 'm1' as message_id, 99 as feature_definition_key").first();
        assertThatThrownBy(() -> ViewRowConverter.fromRow(keyedRow, Map.of(0, "DEF_A")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("99");

        Row jsonRow = spark.sql("select 'm1' as message_id, 'DEF_A' as feature_definition").first();
        assertThat(ViewRowConverter.fromRow(jsonRow).getFeatureDefinitionJson()).isEqualTo("DEF_A");
        assertThat(ViewRowConverter.fromRow(jsonRow, Map.of()).getFeatureDefinitionJson()).isEqualTo("DEF_A");
    }

    @Test
    void rangeLayout_keepsFeatureSetsTogether_andSplitsACommonSetAcrossPartitions() {
        spark.conf().set("spark.sql.adaptive.enabled", "false"); // exact partition count for the assertions
        spark.conf().set("spark.sql.shuffle.partitions", "8");
        // 600 messages: 4 feature sets, one of them shared by 450 messages (skew)
        Dataset<Row> joined = spark.sql("select concat('msg-', id) as message_id, "
                + "case when id < 450 then array(named_struct('feature_definition_key', 0)) "
                + "     when id < 500 then array(named_struct('feature_definition_key', 1)) "
                + "     when id < 550 then array(named_struct('feature_definition_key', 2), named_struct('feature_definition_key', 3)) "
                + "     else array(named_struct('feature_definition_key', 4)) end as features "
                + "from range(600)").repartition(8); // start scattered, like after the message_id join

        Dataset<Row> laidOut = FeatureSetLayout.layout(joined, true, true);
        List<List<Row>> partitions = laidOut.javaRDD().glom().collect();

        int nonEmpty = 0;
        int maxRows = 0;
        int totalFeatureSetsTouched = 0;
        Set<String> seen = new HashSet<>();
        for (List<Row> partition : partitions) {
            if (partition.isEmpty()) {
                continue;
            }
            nonEmpty++;
            maxRows = Math.max(maxRows, partition.size());
            Set<String> sets = new HashSet<>();
            for (Row r : partition) {
                sets.add(r.getList(r.fieldIndex("features")).toString());
                seen.add(r.getAs("message_id"));
            }
            totalFeatureSetsTouched += sets.size();
        }
        assertThat(seen).hasSize(600);                 // nothing lost or duplicated
        assertThat(maxRows).isLessThan(450);           // the common set is split, not all in one partition
        assertThat(nonEmpty).isGreaterThan(2);
        // hash-by-message_id layout would make nearly every partition touch all 4 sets (~32); range keeps it low
        assertThat(totalFeatureSetsTouched).isLessThanOrEqualTo(nonEmpty + 4);
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
