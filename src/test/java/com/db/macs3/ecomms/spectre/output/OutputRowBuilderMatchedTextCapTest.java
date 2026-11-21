package com.db.macs3.ecomms.spectre.output;

import com.db.macs3.ecomms.spectre.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.output.MatchedTextJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the matched_text bounding in {@link OutputRowBuilder#buildMatchedTextJson}:
 * a per-term hit cap and a per-hit text-length cap, introduced to stop a term with a huge
 * number of matches from exhausting the executor heap.
 */
class OutputRowBuilderMatchedTextCapTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_HITS = OutputRowBuilder.MAX_REPORTED_HITS_PER_TERM;
    private static final int MAX_CHARS = OutputRowBuilder.MAX_REPORTED_MATCH_TEXT_CHARS;

    private static List<AreaMatch> bodyMatches(int count, String text) {
        List<AreaMatch> matches = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            matches.add(AreaMatch.messageBody(new MatchSpan(i * 10, i * 10 + text.length(), text)));
        }
        return matches;
    }

    private static MatchedTextJson parse(String json) throws Exception {
        return MAPPER.readValue(json, MatchedTextJson.class);
    }

    private static MatchedTextJson.HitDetail detail(String json) throws Exception {
        MatchedTextJson parsed = parse(json);
        assertThat(parsed.getHitDetailsHs()).hasSize(1);
        return parsed.getHitDetailsHs().getFirst();
    }

    @Test
    @DisplayName("hits at or below the cap are all written, unchanged")
    void belowCap_allHitsWritten() throws Exception {
        String json = OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", bodyMatches(MAX_HITS, "bomb"), Map.of());

        MatchedTextJson.HitDetail detail = detail(json);
        assertThat(detail.getMsgText()).hasSize(MAX_HITS);
        assertThat(detail.getMsgText().getFirst()).isEqualTo(new MatchedTextJson.TextHit("bomb", 0, 4));
        assertThat(detail.getMsgText().get(MAX_HITS - 1))
                .isEqualTo(new MatchedTextJson.TextHit("bomb", (MAX_HITS - 1) * 10, 4));
    }

    @Test
    @DisplayName("hits above the cap are truncated to the first MAX_REPORTED_HITS_PER_TERM, in order")
    void aboveCap_truncatedToCapKeepingOrder() throws Exception {
        String json = OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", bodyMatches(MAX_HITS + 500, "bomb"), Map.of());

        List<MatchedTextJson.TextHit> hits = detail(json).getMsgText();
        assertThat(hits).hasSize(MAX_HITS);
        assertThat(hits.getFirst().getStart()).isEqualTo(0);
        assertThat(hits.get(MAX_HITS - 1).getStart()).isEqualTo((MAX_HITS - 1) * 10);
    }

    @Test
    @DisplayName("the cap is per term across areas, not per area")
    void cap_appliesAcrossAreas() throws Exception {
        List<AreaMatch> matches = new ArrayList<>();
        for (int i = 0; i < MAX_HITS; i++) {
            matches.add(AreaMatch.messageBody(new MatchSpan(i, i + 1, "a")));
        }
        matches.add(AreaMatch.subject(new MatchSpan(0, 1, "a")));
        matches.add(AreaMatch.attachment("att-1", new MatchSpan(0, 1, "a")));

        MatchedTextJson.HitDetail detail = detail(
                OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of()));

        assertThat(detail.getMsgText()).hasSize(MAX_HITS);
        assertThat(detail.getSubject()).isEmpty();
        assertThat(detail.getAttachmentText()).isEmpty();
    }

    @Test
    @DisplayName("a hit's text longer than the limit is cut, but its real start and length are kept")
    void longHitText_truncatedButSpanPreserved() throws Exception {
        String huge = "x".repeat(MAX_CHARS * 3);
        List<AreaMatch> matches = List.of(AreaMatch.messageBody(new MatchSpan(7, 7 + huge.length(), huge)));

        MatchedTextJson.TextHit hit = detail(
                OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of())).getMsgText().getFirst();

        assertThat(hit.getText()).hasSize(MAX_CHARS).isEqualTo("x".repeat(MAX_CHARS));
        assertThat(hit.getStart()).isEqualTo(7);
        assertThat(hit.getLength()).isEqualTo(huge.length());
    }

    @Test
    @DisplayName("a hit whose text is exactly the limit is not shortened")
    void hitTextAtLimit_untouched() throws Exception {
        String exact = "y".repeat(MAX_CHARS);
        List<AreaMatch> matches = List.of(AreaMatch.messageBody(new MatchSpan(0, MAX_CHARS, exact)));

        MatchedTextJson.TextHit hit = detail(
                OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of())).getMsgText().getFirst();

        assertThat(hit.getText()).isEqualTo(exact);
    }

    @Test
    @DisplayName("a null matched text does not throw and is written as null")
    void nullMatchedText_tolerated() throws Exception {
        List<AreaMatch> matches = List.of(AreaMatch.messageBody(new MatchSpan(0, 4, null)));

        MatchedTextJson.TextHit hit = detail(
                OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of())).getMsgText().getFirst();

        assertThat(hit.getText()).isNull();
        assertThat(hit.getLength()).isEqualTo(4);
    }

    @Test
    @DisplayName("an over-cap term still routes surviving hits to subject and attachments with names")
    void routing_preservedWhenCapped() throws Exception {
        List<AreaMatch> matches = new ArrayList<>();
        matches.add(AreaMatch.subject(new MatchSpan(0, 4, "bomb")));
        matches.add(AreaMatch.attachment("att-1", new MatchSpan(3, 7, "bomb")));
        matches.addAll(bodyMatches(MAX_HITS + 50, "bomb"));

        MatchedTextJson.HitDetail detail = detail(OutputRowBuilder.buildMatchedTextJson(
                "m1", "lex::1", matches, Map.of("att-1", "report.pdf")));

        assertThat(detail.getSubject()).hasSize(1);
        assertThat(detail.getAttachmentText()).hasSize(1);
        assertThat(detail.getAttachmentText().getFirst().getAttachmentName()).isEqualTo("report.pdf");
        assertThat(detail.getMsgText()).hasSize(MAX_HITS - 2);
    }

    @Test
    @DisplayName("the 3-arg overload produces the same JSON as the termId overload")
    void threeArgOverload_equivalent() {
        List<AreaMatch> matches = bodyMatches(MAX_HITS + 5, "bomb");

        assertThat(OutputRowBuilder.buildMatchedTextJson("m1", matches, Map.of()))
                .isEqualTo(OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of()));
    }

    @Test
    @DisplayName("a pathological term (many hits, each very long) yields bounded output")
    void pathologicalTerm_outputIsBounded() {
        String huge = "z".repeat(MAX_CHARS * 5);
        List<AreaMatch> matches = new ArrayList<>();
        for (int i = 0; i < MAX_HITS * 3; i++) {
            matches.add(AreaMatch.messageBody(new MatchSpan(0, huge.length(), huge)));
        }

        String json = OutputRowBuilder.buildMatchedTextJson("m1", "lex::1", matches, Map.of());

        // Roughly MAX_HITS * MAX_CHARS of text plus per-hit JSON overhead; far below the uncapped size.
        assertThat(json.length()).isLessThan(MAX_HITS * (MAX_CHARS + 100));
        assertThat(json.length()).isLessThan(matches.size() * huge.length() / 10);
    }
}
