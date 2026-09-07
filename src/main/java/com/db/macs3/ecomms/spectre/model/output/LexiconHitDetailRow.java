package com.db.macs3.ecomms.spectre.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
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
 *
 * <p>Field order and NOT NULL/NULLABLE mode match the delivered BigQuery
 * schema verbatim (rechecked against the live table, both restricted and
 * unrestricted — identical shape): {@code evaluated_lexicons} precedes
 * {@code dataset_partition_value}; {@link EvaluatedLexicon.TermDtl#getMatchedText}
 * is NULLABLE (the BigQuery column's declared type is JSON — Spark carries it
 * as a {@code StringType} holding valid JSON text, since Spark has no
 * first-class JSON type the BigQuery connector maps this to; BigQuery itself
 * coerces the JSON-text string into the destination JSON column on write).
 */
public class LexiconHitDetailRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private String processId;
    private String pipelineExecId;
    private List<EvaluatedLexicon> evaluatedLexicons;
    private LocalDate datasetPartitionValue;
    private String createdBy;
    private Instant createdTs;

    /**
     * @param messageId                  the message this row is for
     * @param processId                   the process run this row belongs to
     * @param pipelineExecId              the pipeline execution this row belongs to
     * @param evaluatedLexicons            one entry per Lexicon-category group that had at least
     *                                    one surviving (post-suppression) match
     * @param datasetPartitionValue      {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for the
     *                                    dataset this message came from — see {@code ScanMessage} class Javadoc
     * @param createdBy                    the writing job's identity
     * @param createdTs                     write time, UTC
     */
    public LexiconHitDetailRow(String messageId, String processId, String pipelineExecId,
                                List<EvaluatedLexicon> evaluatedLexicons, LocalDate datasetPartitionValue,
                                String createdBy, Instant createdTs) {
        this.messageId = messageId;
        this.processId = processId;
        this.pipelineExecId = pipelineExecId;
        this.evaluatedLexicons = evaluatedLexicons;
        this.datasetPartitionValue = datasetPartitionValue;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getPipelineExecId() {
        return pipelineExecId;
    }

    public void setPipelineExecId(String pipelineExecId) {
        this.pipelineExecId = pipelineExecId;
    }

    public List<EvaluatedLexicon> getEvaluatedLexicons() {
        return evaluatedLexicons;
    }

    public void setEvaluatedLexicons(List<EvaluatedLexicon> evaluatedLexicons) {
        this.evaluatedLexicons = evaluatedLexicons;
    }

    public LocalDate getDatasetPartitionValue() {
        return datasetPartitionValue;
    }

    public void setDatasetPartitionValue(LocalDate datasetPartitionValue) {
        this.datasetPartitionValue = datasetPartitionValue;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Instant getCreatedTs() {
        return createdTs;
    }

    public void setCreatedTs(Instant createdTs) {
        this.createdTs = createdTs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LexiconHitDetailRow)) {
            return false;
        }
        LexiconHitDetailRow other = (LexiconHitDetailRow) obj;
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

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<TermDtl> getTermDtls() {
            return termDtls;
        }

        public void setTermDtls(List<TermDtl> termDtls) {
            this.termDtls = termDtls;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof EvaluatedLexicon)) {
                return false;
            }
            EvaluatedLexicon other = (EvaluatedLexicon) obj;
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

            public String getTermId() {
                return termId;
            }

            public void setTermId(String termId) {
                this.termId = termId;
            }

            public String getMatchedText() {
                return matchedText;
            }

            public void setMatchedText(String matchedText) {
                this.matchedText = matchedText;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (!(obj instanceof TermDtl)) {
                    return false;
                }
                TermDtl other = (TermDtl) obj;
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
