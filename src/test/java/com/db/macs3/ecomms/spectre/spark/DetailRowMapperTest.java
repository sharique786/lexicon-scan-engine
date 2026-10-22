package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link DetailRowMapper}'s own {@code call} logic — no
 * {@code SparkSession} needed, see {@link PipelineRecordAuditRowMapperTest}
 * class Javadoc for why. Covers the three independent skip conditions
 * (errored result, {@code restricted} mismatch, null detail row) documented
 * on the class itself.
 */
@DisplayName("DetailRowMapper")
class DetailRowMapperTest {

    private static final LocalDate DATASET_PARTITION_VALUE = LocalDate.parse("2026-09-08");
    private static final Instant NOW = Instant.parse("2026-09-08T10:00:00Z");

    private static LexiconHitDetailRow detailRow(String messageId) {
        return new LexiconHitDetailRow(
                messageId, "proc-1", "pipe-1", List.of(), DATASET_PARTITION_VALUE, "scan-engine", NOW);
    }

    private static List<Row> mapAll(boolean restricted, List<MessageProcessingResult> input) {
        Iterator<Row> output = new DetailRowMapper(restricted).call(input.iterator());
        List<Row> rows = new ArrayList<>();
        output.forEachRemaining(rows::add);
        return rows;
    }

    @Test
    @DisplayName("a restricted-mapper instance only emits restricted results")
    void restrictedMapperOnlyEmitsRestrictedResults() {
        List<MessageProcessingResult> input = List.of(
                MessageProcessingResult.success("msg-r", true, "2026-09-08", null, detailRow("msg-r"), null),
                MessageProcessingResult.success("msg-u", false, "2026-09-08", null, detailRow("msg-u"), null));

        List<Row> rows = mapAll(true, input);

        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getString(0)).isEqualTo("msg-r");
    }

    @Test
    @DisplayName("an unrestricted-mapper instance only emits unrestricted results")
    void unrestrictedMapperOnlyEmitsUnrestrictedResults() {
        List<MessageProcessingResult> input = List.of(
                MessageProcessingResult.success("msg-r", true, "2026-09-08", null, detailRow("msg-r"), null),
                MessageProcessingResult.success("msg-u", false, "2026-09-08", null, detailRow("msg-u"), null));

        List<Row> rows = mapAll(false, input);

        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getString(0)).isEqualTo("msg-u");
    }

    @Test
    @DisplayName("skips a successful result whose detail row is null (nothing survived suppression)")
    void skipsNullDetailRow() {
        MessageProcessingResult successWithNoDetail =
                MessageProcessingResult.success("msg-1", true, "2026-09-08", null, null, null);

        assertThat(mapAll(true, List.of(successWithNoDetail))).isEmpty();
    }

    @Test
    @DisplayName("skips an errored result entirely, regardless of its restricted flag")
    void skipsErroredResult() {
        MessageProcessingResult failure = MessageProcessingResult.failure("msg-2", true, "2026-09-08", "boom");

        assertThat(mapAll(true, List.of(failure))).isEmpty();
    }
}
