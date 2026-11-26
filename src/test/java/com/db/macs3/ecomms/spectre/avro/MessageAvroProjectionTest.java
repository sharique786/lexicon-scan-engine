package com.db.macs3.ecomms.spectre.avro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link MessageAvroReader#projectScannedFields}: unused AVRO columns must be dropped before the join shuffle, while
 * every field {@code MessageRowConverter} reads is kept (and a field the file does not have is not invented).
 */
@DisplayName("MessageAvroReader column projection")
class MessageAvroProjectionTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]").appName("avro-projection-test")
                .config("spark.ui.enabled", "false").getOrCreate();
    }

    @Test
    @DisplayName("drops top-level columns the scan never reads and keeps the five it does, in a fixed order")
    void dropsUnusedColumns() {
        Dataset<Row> messages = spark.sql("select 'm1' as message_id, named_struct('channel_name','EMAIL') as source, "
                + "named_struct('content', named_struct('subject','s')) as message, array() as attachments, "
                + "named_struct('run_hour','01') as processing, 'huge raw payload' as raw_mime, 7 as other");

        Dataset<Row> projected = MessageAvroReader.projectScannedFields(messages);

        assertThat(projected.columns())
                .containsExactly("message_id", "source", "message", "attachments", "processing");
        assertThat(projected.first().<String>getAs("message_id")).isEqualTo("m1");
    }

    @Test
    @DisplayName("a known field missing from the file stays missing (no failure, nothing invented)")
    void missingKnownFieldStaysMissing() {
        Dataset<Row> messages = spark.sql("select 'm1' as message_id, named_struct('channel_name','CHAT') as source, "
                + "'x' as extra");

        assertThat(MessageAvroReader.projectScannedFields(messages).columns())
                .containsExactly("message_id", "source");
    }

    @Test
    @DisplayName("returns the same Dataset when there is nothing to drop")
    void noOpWhenAlreadyMinimal() {
        Dataset<Row> messages = spark.sql("select 'm1' as message_id, named_struct('channel_name','CHAT') as source");

        assertThat(MessageAvroReader.projectScannedFields(messages)).isSameAs(messages);
    }

    @Test
    @DisplayName("the projected field list covers every top-level field MessageRowConverter reads")
    void coversConverterFields() {
        assertThat(MessageAvroReader.SCANNED_TOP_LEVEL_FIELDS).containsExactlyInAnyOrder(
                AvroConstants.FIELD_MESSAGE_ID, AvroConstants.FIELD_SOURCE, AvroConstants.FIELD_MESSAGE,
                AvroConstants.FIELD_ATTACHMENTS, AvroConstants.FIELD_PROCESSING);
    }
}
