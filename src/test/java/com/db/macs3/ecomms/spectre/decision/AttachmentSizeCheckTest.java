package com.db.macs3.ecomms.spectre.decision;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttachmentSizeCheckTest {

    @Test
    void utf8Length_matchesGetBytes_forAsciiAccentsCjkEmojiAndUnpairedSurrogates() {
        String[] samples = {"", "plain ascii", "caf\u00e9 \u00fc\u00f1", "\u65e5\u672c\u8a9e", "emoji \uD83D\uDE00 end",
                "lone high \uD83D x", "lone low \uDE00 x", "mix \u00e9\u65e5\uD83D\uDE00a"};
        for (String s : samples) {
            assertEquals(s.getBytes(StandardCharsets.UTF_8).length, FeatureScanOrchestrator.utf8Length(s), s);
        }
    }
}
