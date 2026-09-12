package com.db.macs3.ecomms.spectre.bq;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import org.apache.spark.sql.Row;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;

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
 * {@code ClassCastException} at real-query time. {@link #getDateOrNull},
 * {@link #getLongOrNull}, and {@link #getBooleanOrDefault} read those four;
 * every other column is genuinely STRING and stays on {@link #getStringOrNull}.
 */
public final class ViewRowConverter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private ViewRowConverter() {
    }

    public static FeatureDecisionRow fromRow(Row row) {
        return new FeatureDecisionRow(
                getStringOrNull(row, BqColumns.View.PROCESS_ID),
                getStringOrNull(row, BqColumns.View.MESSAGE_ID),
                getDateOrNull(row, BqColumns.View.DATASET_PARTITION),
                getStringOrNull(row, BqColumns.View.FEATURE_TAGGING_TYPE),
                getStringOrNull(row, BqColumns.View.FEATURE_TYPE),
                getLongOrNull(row, BqColumns.View.FEATURE_ID),
                getStringOrNull(row, BqColumns.View.FEATURE_NAME),
                getStringOrNull(row, BqColumns.View.SUB_FEATURE_TYPE),
                getStringOrNull(row, BqColumns.View.FEATURES_TO_APPLY),
                getBooleanOrDefault(row, BqColumns.View.IS_NOISE_REDUCTION),
                getStringOrNull(row, BqColumns.View.OPERATOR),
                getStringOrNull(row, BqColumns.View.FEATURE_DEFINITION),
                getDateOrNull(row, BqColumns.View.FEATURE_PARTITION_VALUE),
                getLongAsStringOrNull(row, BqColumns.View.POLICY_ENGINE_ID)
        );
    }

    private static String getStringOrNull(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        return row.isNullAt(idx) ? null : row.getString(idx);
    }

    private static String getLongAsStringOrNull(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        Long longVal = row.isNullAt(idx) ? null : row.getLong(idx);
        return longVal == null ? null : longVal.toString();
    }

    private static Long getLongOrNull(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        return row.isNullAt(idx) ? null : row.getLong(idx);
    }

    /**
     * {@code is_noise_reduction} is NOT modelled as nullable — a null value reads as {@code false}.
     */
    private static boolean getBooleanOrDefault(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        return !row.isNullAt(idx) && row.getBoolean(idx);
    }

    /**
     * Reads a DATE column as a {@link LocalDate}, tolerating either external
     * representation Spark may hand back depending on
     * {@code spark.sql.datetime.java8API.enabled} (this job does not set
     * that config, so the default {@code false} — {@link java.sql.Date} —
     * path is the one actually exercised, but a {@link LocalDate} value is
     * accepted too rather than assuming one specific setting forever).
     */
    private static LocalDate getDateOrNull(Row row, String columnName) {
        int idx = row.fieldIndex(columnName);
        if (row.isNullAt(idx)) {
            return null;
        }
        Object value = row.get(idx);
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        return ((Date) value).toLocalDate();
    }
}
