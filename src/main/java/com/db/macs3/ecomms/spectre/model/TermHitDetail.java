package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;

/**
 * One entry in {@code evaluated_lexicons.term_dtls} (REPEATED) — represents a
 * single lexicon term that produced at least one Hyperscan match for a given
 * message. Terms that did NOT match are not represented here at all; only
 * hits are recorded (see {@code lexicon-hit-summary} schema).
 *
 * <h2>matched_text JSON structure</h2>
 * <p>{@link #matchedText} is a JSON string (BigQuery JSON column) with one key
 * per scanned text segment ({@code "msg"} for the message body,
 * {@code "attachment-0"}, {@code "attachment-1"}, ... for each attachment):
 * <pre>
 * {
 *   "msg": {
 *     "matches": [
 *       {"term_id":"lexicon_market_cond_2::1","match_text":"bomb",
 *        "start_index":23,"end_index":26,"delta":0}
 *     ]
 *   },
 *   "attachment-0": { "matches": [ ... ] }
 * }
 * </pre>
 *
 * <p>For {@code lexicon-hit-summary} rows where the parent message's
 * {@code message_type = 'restricted'}, {@link #matchedText} is replaced with
 * the literal string {@code "REFER LEXICON HIT RESTRICTED TABLE"} — the real
 * match JSON is instead written to the separate
 * {@code lexicon-hit-restricted} table, which has stricter access controls.
 */
public class TermHitDetail implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String RESTRICTED_PLACEHOLDER = "REFER LEXICON HIT RESTRICTED TABLE";

    private String termId;
    private String termRegexPattern;
    private String matchedText;

    public TermHitDetail() {}

    public static TermHitDetail of(String termId, String termRegexPattern, String matchedTextJson) {
        TermHitDetail d = new TermHitDetail();
        d.termId           = termId;
        d.termRegexPattern = termRegexPattern;
        d.matchedText       = matchedTextJson;
        return d;
    }

    /**
     * Builds the slim variant used in {@code lexicon-hit-restricted}
     * (no {@code term_regex_pattern} column in that table).
     */
    public static TermHitDetail ofRestricted(String termId, String matchedTextJson) {
        TermHitDetail d = new TermHitDetail();
        d.termId      = termId;
        d.matchedText = matchedTextJson;
        return d;
    }

    /** @return a copy of this detail with {@link #matchedText} replaced by the redaction placeholder. */
    public TermHitDetail withRedactedText() {
        return of(this.termId, this.termRegexPattern, RESTRICTED_PLACEHOLDER);
    }

    public String getTermId()                 { return termId; }
    public void setTermId(String v)           { this.termId = v; }
    public String getTermRegexPattern()       { return termRegexPattern; }
    public void setTermRegexPattern(String v) { this.termRegexPattern = v; }
    public String getMatchedText()            { return matchedText; }
    public void setMatchedText(String v)      { this.matchedText = v; }

    @Override
    public String toString() {
        return "TermHitDetail{termId='" + termId + "'}";
    }
}
