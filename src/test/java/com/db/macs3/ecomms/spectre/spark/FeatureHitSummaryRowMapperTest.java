package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.model.output.FeatureHitSummaryRow;
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
 * Exercises {@link FeatureHitSummaryRowMapper}'s own {@code call} logic — no
 * {@code SparkSession} needed, see {@link PipelineRecordAuditRowMapperTest}
 * class Javadoc for why.
 */
@DisplayName("FeatureHitSummaryRowMapper")
class FeatureHitSummaryRowMapperTest {

    private static final LocalDate DATASET_PARTITION_VALUE = LocalDate.parse("2026-09-08");
    private static final Instant NOW = Instant.parse("2026-09-08T10:00:00Z");

    private static FeatureHitSummaryRow featureHitSummaryRow(String messageId) {
        return new FeatureHitSummaryRow(messageId, List.of(), DATASET_PARTITION_VALUE,
                "Lexicon-Tagging", "scan-engine", NOW, "proc-1", "pipe-1");
    }

    private static List<Row> mapAll(List<MessageProcessingResult> input) {
        Iterator<Row> output = new FeatureHitSummaryRowMapper().call(input.iterator());
        List<Row> rows = new ArrayList<>();
        output.forEachRemaining(rows::add);
        return rows;
    }

    @Test
    @DisplayName("maps a successful result to its feature-hit-summary Row")
    void mapsSuccessfulResult() {
        MessageProcessingResult success = MessageProcessingResult.success(
                "msg-1", true, "2026-09-08", null, null, featureHitSummaryRow("msg-1"));

        List<Row> rows = mapAll(List.of(success));

        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getString(0)).isEqualTo("msg-1");
    }

    @Test
    @DisplayName("skips an errored result entirely")
    void skipsErroredResult() {
        MessageProcessingResult failure = MessageProcessingResult.failure("msg-2", false, "2026-09-08", "boom");

        assertThat(mapAll(List.of(failure))).isEmpty();
    }
}
