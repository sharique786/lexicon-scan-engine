package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * One row in the {@code spectre-audit.lexicon-hit-restricted} BigQuery output
 * table. This table exists purely to hold the UNREDACTED match text for
 * messages whose {@code message_type = 'restricted'} — it has tighter access
 * controls than {@code lexicon-hit-summary}, so the two tables intentionally
 * carry different levels of detail.
 *
 * <p>Only messages where {@code message_type = 'restricted'} get a row here.
 * Unrestricted messages are never written to this table at all.
 *
 * <p>Schema (BQ) — deliberately slimmer than {@code lexicon-hit-summary}:
 * <pre>
 * message_id            STRING    NOT NULL
 * process_id            STRING    NOT NULL
 * pipeline_exec_id      STRING    NOT NULL
 * evaluated_lexicons    RECORD REPEATED
 *   id                  STRING
 *   term_dtls           RECORD REPEATED
 *     term_id           STRING
 *     matched_text      JSON      (real, UNREDACTED match JSON)
 * created_by            STRING
 * created_ts            TIMESTAMP
 * </pre>
 *
 * <p>Note: no {@code run_date}, {@code sent_date}, {@code message_type},
 * {@code name}, {@code total_terms_count}, {@code regex_hit_count}, or
 * {@code term_regex_pattern} columns — this table only exists to answer
 * "what was the exact matched text" for an already-identified restricted hit;
 * everything else is available by joining back to {@code lexicon-hit-summary}
 * on {@code message_id}.
 */
public class LexiconHitRestrictedRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private String messageId;
    private String processId;
    private String pipelineExecId;
    private List<RestrictedEvaluatedLexicon> evaluatedLexicons = new ArrayList<>();
    private String createdBy = "SYSTEM";
    private Timestamp createdTs;

    public LexiconHitRestrictedRow() {}

    public String getMessageId()             { return messageId; }
    public void setMessageId(String v)      { this.messageId = v; }
    public String getProcessId()             { return processId; }
    public void setProcessId(String v)      { this.processId = v; }
    public String getPipelineExecId()        { return pipelineExecId; }
    public void setPipelineExecId(String v) { this.pipelineExecId = v; }
    public List<RestrictedEvaluatedLexicon> getEvaluatedLexicons() { return evaluatedLexicons; }
    public void setEvaluatedLexicons(List<RestrictedEvaluatedLexicon> v) {
        this.evaluatedLexicons = v != null ? v : new ArrayList<>();
    }
    public String getCreatedBy()             { return createdBy; }
    public void setCreatedBy(String v)      { this.createdBy = v; }
    public Timestamp getCreatedTs()          { return createdTs; }
    public void setCreatedTs(Timestamp v)   { this.createdTs = v; }

    @Override
    public String toString() {
        return "LexiconHitRestrictedRow{messageId='" + messageId + "', lexicons=" + evaluatedLexicons.size() + '}';
    }

    /**
     * Nested REPEATED entry within {@code lexicon-hit-restricted.evaluated_lexicons}.
     * Slimmer than {@link EvaluatedLexicon} — only {@code id} and {@code term_dtls}.
     */
    public static class RestrictedEvaluatedLexicon implements Serializable {
        private static final long serialVersionUID = 1L;

        private String id;
        private List<TermHitDetail> termDtls = new ArrayList<>();

        public RestrictedEvaluatedLexicon() {}

        public static RestrictedEvaluatedLexicon of(String id, List<TermHitDetail> termDtls) {
            RestrictedEvaluatedLexicon e = new RestrictedEvaluatedLexicon();
            e.id       = id;
            e.termDtls = termDtls != null ? termDtls : new ArrayList<>();
            return e;
        }

        public String getId()                    { return id; }
        public void setId(String v)              { this.id = v; }
        public List<TermHitDetail> getTermDtls() { return termDtls; }
        public void setTermDtls(List<TermHitDetail> v) { this.termDtls = v != null ? v : new ArrayList<>(); }
    }
}
