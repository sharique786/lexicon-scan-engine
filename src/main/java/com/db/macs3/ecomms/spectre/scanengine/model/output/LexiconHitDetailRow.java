package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Shared row shape for {@code lexicon-hit-restricted} and
 * {@code lexicon-hit-unrestricted} — identical schema, split purely by
 * source GCS path: a message read from a {@code restricted/} subfolder
 * writes here; one from {@code unrestricted/} writes to the unrestricted
 * table. Which table a given row is destined for is a WRITE-TIME decision
 * (see {@code OutputTableWriter}), not encoded in this class itself.
 *
 * <p>Unlike {@link LexiconHitSummaryRow}, only Lexicon-category (post
 * disclaimer-suppression) matches are represented here — see
 * {@code OutputRowBuilder} — since this table exists specifically to carry
 * {@link EvaluatedLexicon.TermDtl#getMatchedText} detail for genuine hits, not
 * a broad per-group summary.
 */
public class LexiconHitDetailRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private String processId;
    private String pipelineExecId;
    private String datasetPartitionValue;
    private List<EvaluatedLexicon> evaluatedLexicons;
    private String createdBy;
    private Instant createdTs;

    /**
     * @param messageId                  the message this row is for
     * @param processId                   the process run this row belongs to
     * @param pipelineExecId              the pipeline execution this row belongs to
     * @param datasetPartitionValue      the Airflow-supplied partition this message's dataset was read under
     * @param evaluatedLexicons            one entry per Lexicon-category group that had at least
     *                                    one surviving (post-suppression) match
     * @param createdBy                    the writing job's identity
     * @param createdTs                     write time, UTC
     */
    public LexiconHitDetailRow(String messageId, String processId, String pipelineExecId,
                                String datasetPartitionValue, List<EvaluatedLexicon> evaluatedLexicons,
                                String createdBy, Instant createdTs) {
        this.messageId = messageId;
        this.processId = processId;
        this.pipelineExecId = pipelineExecId;
        this.datasetPartitionValue = datasetPartitionValue;
        this.evaluatedLexicons = evaluatedLexicons;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getProcessId() { return processId; }
    public void setProcessId(String processId) { this.processId = processId; }
    public String getPipelineExecId() { return pipelineExecId; }
    public void setPipelineExecId(String pipelineExecId) { this.pipelineExecId = pipelineExecId; }
    public String getDatasetPartitionValue() { return datasetPartitionValue; }
    public void setDatasetPartitionValue(String datasetPartitionValue) { this.datasetPartitionValue = datasetPartitionValue; }
    public List<EvaluatedLexicon> getEvaluatedLexicons() { return evaluatedLexicons; }
    public void setEvaluatedLexicons(List<EvaluatedLexicon> evaluatedLexicons) { this.evaluatedLexicons = evaluatedLexicons; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public Instant getCreatedTs() { return createdTs; }
    public void setCreatedTs(Instant createdTs) { this.createdTs = createdTs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LexiconHitDetailRow)) {
            return false;
        }
        LexiconHitDetailRow other = (LexiconHitDetailRow) o;
        return Objects.equals(messageId, other.messageId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(evaluatedLexicons, other.evaluatedLexicons)
                && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, processId, pipelineExecId, datasetPartitionValue,
                evaluatedLexicons, createdBy, createdTs);
    }

    @Override
    public String toString() {
        return "LexiconHitDetailRow[messageId=" + messageId + ", processId=" + processId
                + ", pipelineExecId=" + pipelineExecId + ", datasetPartitionValue=" + datasetPartitionValue
                + ", evaluatedLexicons=" + evaluatedLexicons + ", createdBy=" + createdBy
                + ", createdTs=" + createdTs + "]";
    }

    public static class EvaluatedLexicon implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String id;
        private List<TermDtl> termDtls;

        /**
         * @param id          the group's {@code feature_id}
         * @param termDtls     one entry per distinct term with a surviving match
         */
        public EvaluatedLexicon(String id, List<TermDtl> termDtls) {
            this.id = id;
            this.termDtls = termDtls;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public List<TermDtl> getTermDtls() { return termDtls; }
        public void setTermDtls(List<TermDtl> termDtls) { this.termDtls = termDtls; }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EvaluatedLexicon)) {
                return false;
            }
            EvaluatedLexicon other = (EvaluatedLexicon) o;
            return Objects.equals(id, other.id) && Objects.equals(termDtls, other.termDtls);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, termDtls);
        }

        @Override
        public String toString() {
            return "EvaluatedLexicon[id=" + id + ", termDtls=" + termDtls + "]";
        }

        public static class TermDtl implements Serializable {

            @Serial
            private static final long serialVersionUID = 1L;

            private String termId;
            private String matchedText;

            /**
             * @param termId          {@code <feature>::<index>}
             * @param matchedText    the serialised {@link MatchedTextJson} for this term — see that
             *                        class for the exact structure
             */
            public TermDtl(String termId, String matchedText) {
                this.termId = termId;
                this.matchedText = matchedText;
            }

            public String getTermId() { return termId; }
            public void setTermId(String termId) { this.termId = termId; }
            public String getMatchedText() { return matchedText; }
            public void setMatchedText(String matchedText) { this.matchedText = matchedText; }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof TermDtl)) {
                    return false;
                }
                TermDtl other = (TermDtl) o;
                return Objects.equals(termId, other.termId) && Objects.equals(matchedText, other.matchedText);
            }

            @Override
            public int hashCode() {
                return Objects.hash(termId, matchedText);
            }

            @Override
            public String toString() {
                return "TermDtl[termId=" + termId + ", matchedText=" + matchedText + "]";
            }
        }
    }
}
