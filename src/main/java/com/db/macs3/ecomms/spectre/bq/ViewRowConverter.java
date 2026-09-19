package com.db.macs3.ecomms.spectre.bq;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.util.RowReaders;
import org.apache.spark.sql.Row;

import java.io.Serial;
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
 *
 * <p><b>Not every column is STRING (rechecked against the live view):</b>
 * {@code dataset_partition}/{@code feature_partition_value} are DATE,
 * {@code feature_id} is LONG (INTEGER), and {@code is_noise_reduction} is
 * BOOLEAN — reading any of these four via {@code Row.getString} throws
 * {@code ClassCastException} at real-query time. {@link RowReaders#getDateOrNull},
 * {@link RowReaders#getLongOrNull}, and {@link RowReaders#getBooleanOrDefault}
 * read those four; every other column is genuinely STRING and stays on
 * {@link RowReaders#getStringOrNull}.
 */
public final class ViewRowConverter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private ViewRowConverter() {
    }

    public static FeatureDecisionRow fromRow(Row row) {
        return new FeatureDecisionRow(
                RowReaders.getStringOrNull(row, BqColumns.View.PROCESS_ID),
                RowReaders.getStringOrNull(row, BqColumns.View.MESSAGE_ID),
                RowReaders.getDateOrNull(row, BqColumns.View.DATASET_PARTITION),
                RowReaders.getStringOrNull(row, BqColumns.View.FEATURE_TAGGING_TYPE),
                RowReaders.getStringOrNull(row, BqColumns.View.FEATURE_TYPE),
                RowReaders.getLongOrNull(row, BqColumns.View.FEATURE_ID),
                RowReaders.getStringOrNull(row, BqColumns.View.FEATURE_NAME),
                RowReaders.getStringOrNull(row, BqColumns.View.SUB_FEATURE_TYPE),
                RowReaders.getStringOrNull(row, BqColumns.View.FEATURES_TO_APPLY),
                RowReaders.getBooleanOrDefault(row, BqColumns.View.IS_NOISE_REDUCTION),
                RowReaders.getStringOrNull(row, BqColumns.View.OPERATOR),
                RowReaders.getStringOrNull(row, BqColumns.View.FEATURE_DEFINITION),
                RowReaders.getDateOrNull(row, BqColumns.View.FEATURE_PARTITION_VALUE),
                RowReaders.getLongAsStringOrNull(row, BqColumns.View.POLICY_ENGINE_ID)
        );
    }
}
