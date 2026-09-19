package com.db.macs3.ecomms.spectre.util;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

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

    private static final Logger log = LoggerFactory.getLogger(RowReaders.class);

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

    /**
     * Reads a UTC timestamp field as an {@link Instant}, tolerating a plain AVRO {@code string}
     * ({@link #parseUtcInstantOrNull}) as well as a {@code timestamp-*} logical type, which Spark
     * hands back as {@link Timestamp} (the default) or {@link Instant}.
     *
     * @return null when the field is absent/null, or is a string that cannot be parsed — an
     * unreadable timestamp must never fail the whole message (or, since conversion runs outside
     * the per-message try/catch, the whole partition), so it is logged at DEBUG only, to avoid one
     * line per message if an entire feed uses an unexpected format
     */
    public static Instant getUtcInstantOrNull(Row row, String fieldName) {
        if (!hasNonNullField(row, fieldName)) {
            return null;
        }
        Object value = row.getAs(fieldName);
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        if (value instanceof String text) {
            Instant parsed = parseUtcInstantOrNull(text);
            if (parsed == null) {
                log.debug("Field '{}' has an unparseable timestamp value '{}' — treating as null", fieldName, text);
            }
            return parsed;
        }
        throw new IllegalStateException(
                "Unsupported type for timestamp field '" + fieldName + "': " + value.getClass().getName());
    }

    /**
     * Parses ISO-8601 text — with an offset/{@code Z} ({@code 2026-09-08T10:15:30Z},
     * {@code 2026-09-08T10:15:30+02:00}) or without one ({@code 2026-09-08T10:15:30.123}, read as
     * UTC since the source field is by definition UTC) — with a space accepted in place of the
     * {@code T}. @return null if blank or in any other format.
     */
    static Instant parseUtcInstantOrNull(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String normalised = text.trim().replace(' ', 'T');
        try {
            return OffsetDateTime.parse(normalised).toInstant();
        } catch (DateTimeParseException ignored) {
            // no offset — fall through to the offset-less form
        }
        try {
            return LocalDateTime.parse(normalised).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
