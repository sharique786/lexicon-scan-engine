package com.db.macs3.ecomms.spectre.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code RowReaders}' UTC-timestamp reader, against schema-carrying
 * {@link GenericRowWithSchema} rows — no {@code SparkSession} needed.
 */
@DisplayName("RowReaders")
class RowReadersTest {

    private static Row rowOf(DataType type, Object value) {
        StructType schema = new StructType(new StructField[]{DataTypes.createStructField("f", type, true)});
        return new GenericRowWithSchema(new Object[]{value}, schema);
    }

    @Nested
    @DisplayName("getUtcInstantOrNull")
    class UtcInstant {

        private static final Instant EXPECTED = Instant.parse("2026-09-08T10:15:30Z");

        private Instant read(String text) {
            return RowReaders.getUtcInstantOrNull(rowOf(DataTypes.StringType, text), "f");
        }

        @Test
        @DisplayName("ISO-8601 with Z, with an offset, and with fractional seconds")
        void isoWithOffset() {
            assertThat(read("2026-09-08T10:15:30Z")).isEqualTo(EXPECTED);
            assertThat(read("2026-09-08T12:15:30+02:00")).isEqualTo(EXPECTED);
            assertThat(read("2026-09-08T10:15:30.250Z")).isEqualTo(EXPECTED.plusMillis(250));
        }

        @Test
        @DisplayName("no offset is read as UTC — the source field is UTC by definition — with T or a space")
        void noOffsetIsUtc() {
            assertThat(read("2026-09-08T10:15:30")).isEqualTo(EXPECTED);
            assertThat(read("2026-09-08 10:15:30")).isEqualTo(EXPECTED);
            assertThat(read("2026-09-08 10:15:30.250")).isEqualTo(EXPECTED.plusMillis(250));
        }

        @Test
        @DisplayName("surrounding whitespace is tolerated")
        void trimmed() {
            assertThat(read("  2026-09-08T10:15:30Z  ")).isEqualTo(EXPECTED);
        }

        @Test
        @DisplayName("blank, null, and unparseable text give null instead of throwing")
        void unparseableIsNull() {
            assertThat(read("")).isNull();
            assertThat(read("   ")).isNull();
            assertThat(read(null)).isNull();
            assertThat(read("not a timestamp")).isNull();
            assertThat(read("08/09/2026 10:15")).isNull();
        }

        @Test
        @DisplayName("a timestamp logical type (Timestamp or Instant) is accepted too")
        void timestampLogicalType() {
            assertThat(RowReaders.getUtcInstantOrNull(
                    rowOf(DataTypes.TimestampType, Timestamp.from(EXPECTED)), "f")).isEqualTo(EXPECTED);
            assertThat(RowReaders.getUtcInstantOrNull(
                    rowOf(DataTypes.TimestampType, EXPECTED), "f")).isEqualTo(EXPECTED);
        }

        @Test
        @DisplayName("absent field gives null; an unsupported type throws")
        void absentAndUnsupported() {
            assertThat(RowReaders.getUtcInstantOrNull(rowOf(DataTypes.StringType, "x"), "missing")).isNull();
            assertThatThrownBy(() -> RowReaders.getUtcInstantOrNull(rowOf(DataTypes.IntegerType, 5), "f"))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
