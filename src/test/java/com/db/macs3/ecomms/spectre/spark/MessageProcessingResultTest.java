package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.model.message.MessageMetadata;
import com.db.macs3.ecomms.spectre.model.message.MessageProcessing;
import com.db.macs3.ecomms.spectre.model.message.MessageSource;
import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MessageProcessingResult.withMessageAttributes")
class MessageProcessingResultTest {

    private static final Instant SENT = Instant.parse("2026-09-08T10:15:30Z");

    @Test
    @DisplayName("copies source_name, sent_date and run_date from the message onto a failure result")
    void copiesOntoFailure() {
        ScanMessage message = new ScanMessage("msg-1", new MessageSource("chat", "Bloomberg Chat", "sys", "conv"),
                new MessageMetadata(SENT), null, List.of(), new MessageProcessing(LocalDate.of(2026, 9, 8), "10"), "ds", false);

        MessageProcessingResult result = MessageProcessingResult.failure("msg-1", false, "2026-09-08", "boom")
                .withMessageAttributes(message);

        assertThat(result.getSourceName()).isEqualTo("Bloomberg Chat");
        assertThat(result.getSentDate()).isEqualTo(SENT);
        assertThat(result.getRunDate()).isEqualTo("2026-09-08");
        assertThat(result.isError()).isTrue();
    }

    @Test
    @DisplayName("copies them onto a success result as well")
    void copiesOntoSuccess() {
        ScanMessage message = new ScanMessage("msg-1", new MessageSource("email", "Exchange", "sys", "conv"),
                new MessageMetadata(SENT), null, List.of(), new MessageProcessing(LocalDate.of(2026, 9, 9), "11"), "ds", true);

        MessageProcessingResult result = MessageProcessingResult
                .success("msg-1", true, "2026-09-09", null, null, null)
                .withMessageAttributes(message);

        assertThat(result.getSourceName()).isEqualTo("Exchange");
        assertThat(result.getSentDate()).isEqualTo(SENT);
        assertThat(result.getRunDate()).isEqualTo("2026-09-09");
        assertThat(result.isError()).isFalse();
    }

    @Test
    @DisplayName("a message missing source, metadata and processing gives null attributes, not an NPE")
    void nullSafe() {
        ScanMessage message = new ScanMessage("msg-1", null, null, List.of(), null, "ds", false);

        MessageProcessingResult result = MessageProcessingResult
                .success("msg-1", false, "2026-09-08", null, null, null)
                .withMessageAttributes(message);

        assertThat(result.getSourceName()).isNull();
        assertThat(result.getSentDate()).isNull();
        assertThat(result.getRunDate()).isNull();
    }
}
