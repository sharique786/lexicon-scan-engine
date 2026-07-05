package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * One row in the {@code spectre-audit.lexicon-hit-summary} BigQuery output
 * table. Unlike the previous flat-row design, this is ONE ROW PER MESSAGE —
 * {@link #evaluatedLexicons} is a REPEATED nested struct with one entry per
 * lexicon feature (or lexicon sub-feature of a composite) that was evaluated
 * for this message, each carrying its own REPEATED {@code term_dtls}.
 *
 * <p>Schema (BQ):
 * <pre>
 * message_id            STRING    NOT NULL
 * run_date              STRING    NOT NULL
 * process_id            STRING    NOT NULL
 * pipeline_exec_id      STRING    NOT NULL
 * sent_date             STRING
 * message_type          STRING               ("unrestricted" | "restricted")
 * evaluated_lexicons    RECORD REPEATED       (see {@link EvaluatedLexicon})
 * created_by            STRING
 * created_ts            TIMESTAMP
 * </pre>
 *
 * <h2>Restricted-message masking</h2>
 * <p>When {@link #messageType} is {@code "restricted"}, every
 * {@code term_dtls[*].matched_text} value is replaced with the literal
 * placeholder {@link TermHitDetail#RESTRICTED_PLACEHOLDER} — the real match
 * text is instead written to the separate
 * {@code lexicon-hit-restricted} table (see {@link LexiconHitRestrictedRow}),
 * which is access-controlled more tightly. Unrestricted messages do NOT get a
 * corresponding row in {@code lexicon-hit-restricted} at all.
 */
public class LexiconHitSummaryRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private String messageId;
    private String runDate;
    private String processId;
    private String pipelineExecId;
    private String sentDate;
    private String messageType;
    private List<EvaluatedLexicon> evaluatedLexicons = new ArrayList<>();
    private String createdBy = "SYSTEM";
    private Timestamp createdTs;

    public LexiconHitSummaryRow() {}

    public String getMessageId()                    { return messageId; }
    public void setMessageId(String v)              { this.messageId = v; }
    public String getRunDate()                       { return runDate; }
    public void setRunDate(String v)                { this.runDate = v; }
    public String getProcessId()                     { return processId; }
    public void setProcessId(String v)              { this.processId = v; }
    public String getPipelineExecId()                { return pipelineExecId; }
    public void setPipelineExecId(String v)         { this.pipelineExecId = v; }
    public String getSentDate()                      { return sentDate; }
    public void setSentDate(String v)               { this.sentDate = v; }
    public String getMessageType()                   { return messageType; }
    public void setMessageType(String v)            { this.messageType = v; }
    public List<EvaluatedLexicon> getEvaluatedLexicons() { return evaluatedLexicons; }
    public void setEvaluatedLexicons(List<EvaluatedLexicon> v) {
        this.evaluatedLexicons = v != null ? v : new ArrayList<>();
    }
    public String getCreatedBy()                     { return createdBy; }
    public void setCreatedBy(String v)              { this.createdBy = v; }
    public Timestamp getCreatedTs()                  { return createdTs; }
    public void setCreatedTs(Timestamp v)           { this.createdTs = v; }

    @Override
    public String toString() {
        return "LexiconHitSummaryRow{messageId='" + messageId + "', messageType='" + messageType +
               "', lexicons=" + evaluatedLexicons.size() + '}';
    }
}
