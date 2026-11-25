package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutputRowCounts — matches what the row mappers emit")
class OutputRowCountsTest {

    private static LexiconHitDetailRow detail(String id) {
        return new LexiconHitDetailRow(id, "proc-1", "pipe-1", List.of(), LocalDate.parse("2026-09-08"),
                "scan-engine", Instant.parse("2026-09-08T10:00:00Z"));
    }

    @Test
    @DisplayName("counts results, errors, and restricted/unrestricted detail rows")
    void counts() {
        List<MessageProcessingResult> results = List.of(
                MessageProcessingResult.success("r", true, "2026-09-08", null, detail("r"), null),
                MessageProcessingResult.success("u", false, "2026-09-08", null, detail("u"), null),
                MessageProcessingResult.success("u2", false, "2026-09-08", null, detail("u2"), null),
                MessageProcessingResult.success("none", true, "2026-09-08", null, null, null),
                MessageProcessingResult.failure("f", true, "2026-09-08", "boom"));

        OutputRowCounts c = results.stream().map(OutputRowCounts::of)
                .reduce(OutputRowCounts.ZERO, OutputRowCounts::plus);

        assertThat(c.results()).isEqualTo(5);
        assertThat(c.errors()).isEqualTo(1);
        assertThat(c.summary()).isEqualTo(4);
        assertThat(c.restrictedDetail()).isEqualTo(1);
        assertThat(c.unrestrictedDetail()).isEqualTo(2);
        assertThat(c.recordAudit()).isEqualTo(5);
    }
}
