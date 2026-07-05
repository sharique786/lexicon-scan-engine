package com.db.macs3.ecomms.spectre.model;

import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Model Business Logic Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ModelLogicTest {

    // ── FeatureDecisionRow ────────────────────────────────────────────────────

    @Test @Order(1)
    @DisplayName("FeatureDecisionRow.isNoiseReduction() is true only for 'Y' (case-insensitive)")
    void isNoiseReduction_caseInsensitive() {
        FeatureDecisionRow rowY  = row("Y");
        FeatureDecisionRow rowy  = row("y");
        FeatureDecisionRow rowN  = row("N");
        FeatureDecisionRow rowNull = row(null);

        assertThat(rowY.isNoiseReduction()).isTrue();
        assertThat(rowy.isNoiseReduction()).isTrue();
        assertThat(rowN.isNoiseReduction()).isFalse();
        assertThat(rowNull.isNoiseReduction()).isFalse();
    }

    @Test @Order(2)
    @DisplayName("FeatureDecisionRow.isRestrictedMessage() is true only for 'restricted' (case-insensitive)")
    void isRestrictedMessage_caseInsensitive() {
        assertThat(msgTypeRow("restricted").isRestrictedMessage()).isTrue();
        assertThat(msgTypeRow("RESTRICTED").isRestrictedMessage()).isTrue();
        assertThat(msgTypeRow("unrestricted").isRestrictedMessage()).isFalse();
        assertThat(msgTypeRow(null).isRestrictedMessage()).isFalse();
    }

    private FeatureDecisionRow row(String isNoiseReductionRaw) {
        return FeatureDecisionRow.of("msg-1", "20260101", "proc-1", "pipe-1", "20260101", "unrestricted",
                "1", "lexicon", "feat", null, isNoiseReductionRaw, "lex_a", false, "{}");
    }

    private FeatureDecisionRow msgTypeRow(String messageType) {
        return FeatureDecisionRow.of("msg-1", "20260101", "proc-1", "pipe-1", "20260101", messageType,
                "1", "lexicon", "feat", null, "N", "lex_a", false, "{}");
    }

    // ── TermHitDetail ─────────────────────────────────────────────────────────

    @Test @Order(10)
    @DisplayName("TermHitDetail.withRedactedText() replaces matchedText with the placeholder, keeps other fields")
    void termHitDetail_withRedactedText() {
        TermHitDetail original = TermHitDetail.of("term::1", "(?:bomb)", "{\"msg\":{\"matches\":[]}}");
        TermHitDetail redacted = original.withRedactedText();

        assertThat(redacted.getTermId()).isEqualTo("term::1");
        assertThat(redacted.getTermRegexPattern()).isEqualTo("(?:bomb)");
        assertThat(redacted.getMatchedText()).isEqualTo(TermHitDetail.RESTRICTED_PLACEHOLDER);
        // Original must remain unchanged (immutable-style operation)
        assertThat(original.getMatchedText()).isNotEqualTo(TermHitDetail.RESTRICTED_PLACEHOLDER);
    }

    @Test @Order(11)
    @DisplayName("TermHitDetail.ofRestricted() omits term_regex_pattern")
    void termHitDetail_ofRestricted() {
        TermHitDetail restricted = TermHitDetail.ofRestricted("term::1", "{\"real\":\"json\"}");
        assertThat(restricted.getTermId()).isEqualTo("term::1");
        assertThat(restricted.getTermRegexPattern()).isNull();
        assertThat(restricted.getMatchedText()).isEqualTo("{\"real\":\"json\"}");
    }

    // ── EvaluatedLexicon ──────────────────────────────────────────────────────

    @Test @Order(20)
    @DisplayName("EvaluatedLexicon.of() derives regexHitCount from termDtls size")
    void evaluatedLexicon_derivesHitCount() {
        List<TermHitDetail> dtls = List.of(
                TermHitDetail.of("t1", "p1", "{}"),
                TermHitDetail.of("t2", "p2", "{}")
        );
        EvaluatedLexicon lex = EvaluatedLexicon.of("1", "feat_a", 20L, dtls);
        assertThat(lex.getRegexHitCount()).isEqualTo(2);
        assertThat(lex.getTotalTermsCount()).isEqualTo(20L);
        assertThat(lex.hasHit()).isTrue();
    }

    @Test @Order(21)
    @DisplayName("EvaluatedLexicon.hasHit() is false when termDtls is empty")
    void evaluatedLexicon_hasHitFalseWhenEmpty() {
        EvaluatedLexicon lex = EvaluatedLexicon.of("1", "feat_a", 20L, List.of());
        assertThat(lex.hasHit()).isFalse();
        assertThat(lex.getRegexHitCount()).isEqualTo(0);
    }

    @Test @Order(22)
    @DisplayName("EvaluatedLexicon.withRedactedTermDtls() redacts every term but preserves id/name/totalCount")
    void evaluatedLexicon_withRedactedTermDtls() {
        List<TermHitDetail> dtls = List.of(
                TermHitDetail.of("t1", "p1", "real text 1"),
                TermHitDetail.of("t2", "p2", "real text 2")
        );
        EvaluatedLexicon original = EvaluatedLexicon.of("1", "feat_a", 20L, dtls);
        EvaluatedLexicon redacted = original.withRedactedTermDtls();

        assertThat(redacted.getId()).isEqualTo("1");
        assertThat(redacted.getName()).isEqualTo("feat_a");
        assertThat(redacted.getTotalTermsCount()).isEqualTo(20L);
        assertThat(redacted.getTermDtls()).hasSize(2);
        assertThat(redacted.getTermDtls()).allMatch(
                d -> d.getMatchedText().equals(TermHitDetail.RESTRICTED_PLACEHOLDER));
    }

    // ── JobConfig ─────────────────────────────────────────────────────────────

    @Test @Order(30)
    @DisplayName("JobConfig.bqViewRef() concatenates project.dataset.viewName")
    void jobConfig_bqViewRef() {
        JobConfig config = new JobConfig();
        config.setBqProject("proj");
        config.setBqDataset("ds");
        config.setViewName("my_view");
        assertThat(config.bqViewRef()).isEqualTo("proj.ds.my_view");
    }

    @Test @Order(31)
    @DisplayName("JobConfig defaults produce sensible fallback table names when unset")
    void jobConfig_defaults() {
        JobConfig config = new JobConfig();
        assertThat(config.getViewName()).isEqualTo("v_lexicon_scan_engine_input");
        assertThat(config.getInputTables().languageFeatureDecision).isEqualTo("spectre-audit.language-feature-decision");
        assertThat(config.getOutputTables().pipelineStageAudit).isEqualTo("spectre-audit.pipeline_stage_audit");
    }

    // ── MessageScanResult ─────────────────────────────────────────────────────

    @Test @Order(40)
    @DisplayName("MessageScanResult.hasRestrictedRow() reflects null vs non-null restricted row")
    void messageScanResult_hasRestrictedRow() {
        LexiconHitSummaryRow summary = new LexiconHitSummaryRow();
        MessageScanResult withRestricted = MessageScanResult.of(summary, new LexiconHitRestrictedRow(), List.of());
        MessageScanResult withoutRestricted = MessageScanResult.of(summary, null, List.of());

        assertThat(withRestricted.hasRestrictedRow()).isTrue();
        assertThat(withoutRestricted.hasRestrictedRow()).isFalse();
    }

    // ── MessageRecord ─────────────────────────────────────────────────────────

    @Test @Order(50)
    @DisplayName("MessageRecord.hasContent() is false for null or blank raw text")
    void messageRecord_hasContent() {
        MessageRecord withText  = MessageRecord.of("m1", "chat", "20260101", "hello", List.of());
        MessageRecord blankText = MessageRecord.of("m2", "chat", "20260101", "   ", List.of());
        MessageRecord nullText  = MessageRecord.of("m3", "chat", "20260101", null, List.of());

        assertThat(withText.hasContent()).isTrue();
        assertThat(blankText.hasContent()).isFalse();
        assertThat(nullText.hasContent()).isFalse();
    }

    @Test @Order(51)
    @DisplayName("MessageRecord.attachmentCount() reflects the attachment list size")
    void messageRecord_attachmentCount() {
        MessageRecord withAttachments = MessageRecord.of("m1", "chat", "20260101", "body", List.of("a1", "a2"));
        assertThat(withAttachments.attachmentCount()).isEqualTo(2);

        MessageRecord noAttachments = MessageRecord.of("m2", "chat", "20260101", "body", List.of());
        assertThat(noAttachments.attachmentCount()).isEqualTo(0);
    }
}
