package com.db.macs3.ecomms.spectre.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * One entry in a lexicon feature's manifest file, mapping Hyperscan's
 * numeric {@code expressionId} back to the human-readable {@code termId}
 * (e.g. {@code "lexicon_market_cond_2::1"}) and the compiled PCRE pattern.
 *
 * <h2>Why this manifest exists</h2>
 * <p>Intel Hyperscan's {@code Expression} class only accepts an integer
 * {@code id} — there is no way to attach an arbitrary string identifier to a
 * compiled expression inside the {@code .hdb} file itself. But the output
 * schema requires reporting the ORIGINAL string {@code term_id} (e.g.
 * {@code "lexicon_market_cond_2::1"}) and {@code term_regex_pattern} for
 * every hit — see {@code lexicon-hit-summary.evaluated_lexicons.term_dtls}.
 *
 * <p>To bridge this gap, the Lexicon Compile Service publishes a small JSON
 * manifest file alongside each {@code .hdb} file on GCS:
 * {@code gs://<bucket>/<prefix>/<featureName>.manifest.json}, containing one
 * entry per successfully-compiled (PASS) term. The Scan Engine loads this
 * manifest together with the {@code .hdb} bytes, broadcasts it to executors,
 * and uses it to resolve {@code expressionId → termId / pattern} at hit time.
 *
 * <p>The manifest's size also directly gives
 * {@code evaluated_lexicons.total_terms_count} — no need to parse Hyperscan's
 * native binary format to discover the term count.
 *
 * <h2>Example manifest file</h2>
 * <pre>
 * [
 *   { "expressionId": 0, "termId": "lexicon_market_cond_2::1",  "pattern": "(?:...)" },
 *   { "expressionId": 1, "termId": "lexicon_market_cond_2::3",  "pattern": "(?:...)" },
 *   { "expressionId": 2, "termId": "lexicon_market_cond_2::10", "pattern": "(?:...)" }
 * ]
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TermManifestEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("expressionId")
    private int expressionId;

    @JsonProperty("termId")
    private String termId;

    @JsonProperty("pattern")
    private String pattern;

    public TermManifestEntry() {}

    public TermManifestEntry(int expressionId, String termId, String pattern) {
        this.expressionId = expressionId;
        this.termId       = termId;
        this.pattern      = pattern;
    }

    public int getExpressionId()          { return expressionId; }
    public void setExpressionId(int v)    { this.expressionId = v; }
    public String getTermId()             { return termId; }
    public void setTermId(String v)       { this.termId = v; }
    public String getPattern()            { return pattern; }
    public void setPattern(String v)      { this.pattern = v; }

    @Override
    public String toString() {
        return "TermManifestEntry{expressionId=" + expressionId + ", termId='" + termId + "'}";
    }
}
