package com.db.macs3.ecomms.spectre.util;

import org.apache.spark.sql.Row;

import java.sql.Date;
import java.time.LocalDate;

/**
 * Shared helpers for reading typed, possibly-null values out of a Spark
 * {@link Row} — used by every converter that turns a raw AVRO or BigQuery
 * {@link Row} into this project's own model classes ({@code MessageRowConverter},
 * {@code ViewRowConverter}). Centralised here so the same null-safety and
 * type-tolerance logic isn't reimplemented — and potentially drifting —
 * separately in every converter.
 *
 * <p>{@link #hasNonNullField} tolerates a field that is entirely absent from
 * {@code row}'s schema (e.g. an optional nested AVRO struct, or a column a
 * BQ view no longer selects), rather than throwing — every other method here
 * builds on it, so the same tolerance applies uniformly.
 */
public final class RowReaders {

    private RowReaders() {
    }

    public static boolean hasNonNullField(Row row, String fieldName) {
        int fieldIndex;
        try {
            fieldIndex = row.fieldIndex(fieldName);
        } catch (IllegalArgumentException e) {
            return false; // field not present in this row's schema at all
        }
        return !row.isNullAt(fieldIndex);
    }

    public static String getStringOrNull(Row row, String fieldName) {
        return hasNonNullField(row, fieldName) ? row.getAs(fieldName) : null;
    }

    public static Long getLongOrNull(Row row, String fieldName) {
        return hasNonNullField(row, fieldName) ? row.getAs(fieldName) : null;
    }

    public static String getLongAsStringOrNull(Row row, String fieldName) {
        Long value = getLongOrNull(row, fieldName);
        return value == null ? null : value.toString();
    }

    /**
     * @return false when the field is null/absent — intended for a column that isn't modelled as nullable.
     */
    public static boolean getBooleanOrDefault(Row row, String fieldName) {
        return hasNonNullField(row, fieldName) && Boolean.TRUE.equals(row.<Boolean>getAs(fieldName));
    }

    /**
     * Reads a {@code date}-logical-type field as a {@link LocalDate},
     * tolerating either external representation Spark may hand back depending
     * on {@code spark.sql.datetime.java8API.enabled}: {@link Date} (the
     * default) or {@link LocalDate} directly.
     */
    public static LocalDate getDateOrNull(Row row, String fieldName) {
        if (!hasNonNullField(row, fieldName)) {
            return null;
        }
        Object value = row.getAs(fieldName);
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        if (value instanceof Date sqlDate) {
            return sqlDate.toLocalDate();
        }
        throw new IllegalStateException(
                "Unsupported type for date field '" + fieldName + "': " + value.getClass().getName());
    }
}
