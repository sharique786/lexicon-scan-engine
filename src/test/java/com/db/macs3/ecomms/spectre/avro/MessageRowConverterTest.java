package com.db.macs3.ecomms.spectre.avro;

import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three attributes {@code pipeline_record_audit} needs from the AVRO message —
 * {@code source.source_name}, {@code message.metadata.start_time_utc},
 * {@code processing.run_date} — as read by {@link MessageRowConverter}. Rows are built with
 * their schema attached ({@link GenericRowWithSchema}), so no {@code SparkSession} is needed;
 * only the fields under test are declared, which also exercises the converter's tolerance for
 * every other AVRO field being absent.
 */
@DisplayName("MessageRowConverter — pipeline_record_audit attributes")
class MessageRowConverterTest {

    private static StructType structOf(Object... nameTypePairs) {
        StructField[] fields = new StructField[nameTypePairs.length / 2];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField((String) nameTypePairs[2 * i], (DataType) nameTypePairs[2 * i + 1], true);
        }
        return new StructType(fields);
    }

    private static Row row(StructType schema, Object... values) {
        return new GenericRowWithSchema(values, schema);
    }

    private static final StructType SOURCE = structOf("source_name", DataTypes.StringType);

    private static Row fullMessage(Object startTimeUtc, DataType startTimeType, Object runDate, DataType runDateType) {
        StructType metadata = structOf("start_time_utc", startTimeType);
        StructType message = structOf("metadata", metadata);
        StructType processing = structOf("run_date", runDateType, "run_hour", DataTypes.StringType);
        StructType schema = structOf("message_id", DataTypes.StringType, "source", SOURCE,
                "message", message, "processing", processing);
        return row(schema, "msg-1", row(SOURCE, "Bloomberg Chat"),
                row(message, row(metadata, startTimeUtc)),
                row(processing, runDate, "10"));
    }

    @Test
    @DisplayName("start_time_utc (string) maps to an Instant and run_date (date) to a LocalDate")
    void mappedFields() {
        ScanMessage message = MessageRowConverter.fromRow(
                fullMessage("2026-09-08T10:15:30Z", DataTypes.StringType, Date.valueOf("2026-09-08"), DataTypes.DateType),
                "2026-09-08", false);

        assertThat(message.getSource().getSourceName()).isEqualTo("Bloomberg Chat");
        assertThat(message.getMetadata().getStartTimeUtc()).isEqualTo(Instant.parse("2026-09-08T10:15:30Z"));
        assertThat(message.getProcessing().getRunDate()).isEqualTo(LocalDate.of(2026, 9, 8));
        assertThat(message.getProcessing().getRunHour()).isEqualTo("10");
    }

    @Test
    @DisplayName("run_date is read the same whether Spark hands back java.sql.Date or LocalDate (java8API mode)")
    void runDateEitherExternalRepresentation() {
        ScanMessage message = MessageRowConverter.fromRow(
                fullMessage("2026-09-08T10:15:30Z", DataTypes.StringType, LocalDate.of(2026, 9, 8), DataTypes.DateType),
                "2026-09-08", false);

        assertThat(message.getProcessing().getRunDate()).isEqualTo(LocalDate.of(2026, 9, 8));
    }

    @Test
    @DisplayName("an unparseable start_time_utc leaves sent_date null without failing the message")
    void unparseableStartTime() {
        ScanMessage message = MessageRowConverter.fromRow(
                fullMessage("yesterday-ish", DataTypes.StringType, Date.valueOf("2026-09-08"), DataTypes.DateType),
                "2026-09-08", false);

        assertThat(message.getMetadata().getStartTimeUtc()).isNull();
        assertThat(message.getSource().getSourceName()).isEqualTo("Bloomberg Chat");
        assertThat(message.getProcessing().getRunDate()).isEqualTo(LocalDate.of(2026, 9, 8));
    }

    @Test
    @DisplayName("absent source/message.metadata/processing blocks give nulls, never an exception")
    void absentBlocks() {
        StructType schema = structOf("message_id", DataTypes.StringType);

        ScanMessage message = MessageRowConverter.fromRow(row(schema, "msg-2"), "2026-09-08", false);

        assertThat(message.getMessageId()).isEqualTo("msg-2");
        assertThat(message.getSource()).isNull();
        assertThat(message.getMetadata()).isNull();
        assertThat(message.getProcessing()).isNull();
    }

    @Test
    @DisplayName("message present but without a metadata block gives null metadata")
    void messageWithoutMetadata() {
        StructType content = structOf("subject", DataTypes.StringType);
        StructType message = structOf("content", content);
        StructType schema = structOf("message_id", DataTypes.StringType, "message", message);

        ScanMessage scanMessage = MessageRowConverter.fromRow(
                row(schema, "msg-3", row(message, row(content, "hi"))), "2026-09-08", false);

        assertThat(scanMessage.getMetadata()).isNull();
        assertThat(scanMessage.getContent().getSubject()).isEqualTo("hi");
    }
}
