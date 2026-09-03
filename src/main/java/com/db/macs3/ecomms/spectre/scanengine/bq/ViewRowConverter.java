package com.db.macs3.ecomms.spectre.scanengine.bq;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Converts one Spark {@link Row} of {@code vw_src_msg_lexicon_decision_mapping}
 * query results into a {@link FeatureDecisionRow}.
 *
 * <p>Kept as a small, standalone, side-effect-free function specifically so
 * the mapping from BQ column name to {@link FeatureDecisionRow} field is in
 * exactly one place (see {@code BqColumns.View} for the column name
 * constants this reads by) — every other class works with
 * {@link FeatureDecisionRow} directly and has no knowledge of the
 * underlying column names at all.
 */
public final class ViewRowConverter implements Serializable {

    private ViewRowConverter() {}

    public static FeatureDecisionRow fromRow(Row row) {
        return new FeatureDecisionRow(
                getStringOrNull(row, BqColumns.View.PROCESS_ID),
                getStringOrNull(row, BqColumns.View.MESSAGE_ID),
                getStringOrNull(row, BqColumns.View.DATASET_PARTITION),
                getStringOrNull(row, BqColumns.View.FEATURE_TAGGING_TYPE),
                getStringOrNull(row, BqColumns.View.FEATURE_TYPE),
                getStringOrNull(row, BqColumns.View.FEATURE_ID),
                getStringOrNull(row, BqColumns.View.FEATURE_NAME),
                getStringOrNull(row, BqColumns.View.SUB_FEATURE_TYPE),
                getStringOrNull(row, BqColumns.View.FEATURES_TO_APPLY),
                getStringOrNull(row, BqColumns.View.IS_NOISE_REDUCTION),
                getStringOrNull(row, BqColumns.View.OPERATOR),
                getStringOrNull(row, BqColumns.View.FEATURE_DEFINITION),
                getStringOrNull(row, BqColumns.View.FEATURE_PARTITION_VALUE),
                getStringOrNull(row, BqColumns.View.POLICY_ENGINE_ID)
        );
    }

    private static String getStringOrNull(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        return row.isNullAt(idx) ? null : row.getString(idx);
    }
}
