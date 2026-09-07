package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * One row of {@code lexicon-hit-summary} — per-message summary of every
 * evaluated feature (regardless of type: standard Lexicon, Disclaimer, or
 * NoiseReduction) and which of its terms matched.
 *
 * <h2>One entry per evaluated FEATURE GROUP, not per sub-feature member</h2>
 * <p>{@link EvaluatedLexicon#getId}/{@link EvaluatedLexicon#getName} are the
 * view's {@code feature_id}/{@code feature_name} (confirmed) — which are
 * shared across every sub-feature member of a composite/NoiseReduction
 * group. One {@link EvaluatedLexicon} entry is therefore built per
 * EVALUATED GROUP (see {@code DecisionTreeEvaluator}'s
 * {@code GroupEvaluationResult}), aggregating every member's own matched
 * terms into its {@link EvaluatedLexicon#getTermDtls} — each {@code term_id}
 * still identifies exactly which underlying lexicon (which member's
 * {@code body.lexiconName}) it came from, since {@code term_id} is
 * {@code <feature>::<index>}.
 *
 * <p>Only groups that were ACTUALLY evaluated appear — a NoiseReduction
 * group short-circuit means every later group (further NoiseReduction, the
 * Disclaimer group, all Lexicon groups) simply has no entry at all here,
 * since {@code DecisionTreeEvaluator} never evaluated them.
 *
 * <p>Field order and NOT NULL/NULLABLE mode match the delivered BigQuery
 * schema verbatim (rechecked against the live table): {@code evaluated_lexicons}
 * precedes {@code dataset_partition_value}; {@link EvaluatedLexicon#getTotalTermsCount}/
 * {@link EvaluatedLexicon#getRegexHitCount}/{@link EvaluatedLexicon.TermDtl#getRegexMatchHitCount}
 * are NULLABLE (hence boxed {@link Long}, not primitive {@code long}) even
 * though this engine always computes a real value for them today.
 */
public class LexiconHitSummaryRow implements Serializable {

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
     * @param messageId              the message this summary is for
     * @param processId                the process run this row belongs to
     * @param pipelineExecId           the pipeline execution this row belongs to
     * @param evaluatedLexicons        one entry per evaluated feature group
     * @param datasetPartitionValue    {@code RuntimeArgs.DatasetDetail#datasetPartitionValue()} for the
     *                                dataset this message came from — see {@code ScanMessage} class Javadoc
     * @param createdBy                 the writing job's identity
     * @param createdTs                  write time, UTC
     */
    public LexiconHitSummaryRow(String messageId, String processId, String pipelineExecId,
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
        if (!(obj instanceof LexiconHitSummaryRow)) {
            return false;
        }
        LexiconHitSummaryRow other = (LexiconHitSummaryRow) obj;
        return Objects.equals(messageId, other.messageId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(evaluatedLexicons, other.evaluatedLexicons)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, processId, pipelineExecId, evaluatedLexicons, datasetPartitionValue,
                createdBy, createdTs);
    }

    @Override
    public String toString() {
        return "LexiconHitSummaryRow[messageId=" + messageId + ", processId=" + processId
                + ", pipelineExecId=" + pipelineExecId + ", evaluatedLexicons=" + evaluatedLexicons
                + ", datasetPartitionValue=" + datasetPartitionValue
                + ", createdBy=" + createdBy + ", createdTs=" + createdTs + "]";
    }

    public static class EvaluatedLexicon implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String id;
        private String name;
        private Long totalTermsCount;
        private Long regexHitCount;
        private List<TermDtl> termDtls;

        /**
         * @param id                 the group's {@code feature_id}
         * @param name                the group's {@code feature_name}
         * @param totalTermsCount    sum of every member's {@code feature_definition.body.totalTermsCount} —
         *                            NULLABLE per the delivered schema
         * @param regexHitCount       count of DISTINCT {@code term_id}s that matched across every
         *                             member of this group (the length of {@link #getTermDtls}) —
         *                             NULLABLE per the delivered schema
         * @param termDtls             one entry per distinct term that matched, across every member
         */
        public EvaluatedLexicon(String id, String name, Long totalTermsCount, Long regexHitCount,
                                 List<TermDtl> termDtls) {
            this.id = id;
            this.name = name;
            this.totalTermsCount = totalTermsCount;
            this.regexHitCount = regexHitCount;
            this.termDtls = termDtls;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getTotalTermsCount() {
            return totalTermsCount;
        }

        public void setTotalTermsCount(Long totalTermsCount) {
            this.totalTermsCount = totalTermsCount;
        }

        public Long getRegexHitCount() {
            return regexHitCount;
        }

        public void setRegexHitCount(Long regexHitCount) {
            this.regexHitCount = regexHitCount;
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
            return Objects.equals(totalTermsCount, other.totalTermsCount)
                    && Objects.equals(regexHitCount, other.regexHitCount)
                    && Objects.equals(id, other.id)
                    && Objects.equals(name, other.name)
                    && Objects.equals(termDtls, other.termDtls);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, totalTermsCount, regexHitCount, termDtls);
        }

        @Override
        public String toString() {
            return "EvaluatedLexicon[id=" + id + ", name=" + name + ", totalTermsCount=" + totalTermsCount
                    + ", regexHitCount=" + regexHitCount + ", termDtls=" + termDtls + "]";
        }
    }

    public static class TermDtl implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String termId;
        private String termRegexPattern;
        private Long regexMatchHitCount;

        /**
         * @param termId                  {@code <feature>::<index>} — see {@code TermIdBuilder}
         * @param termRegexPattern       the compiled Hyperscan pattern text, for auditability
         * @param regexMatchHitCount     how many times this term's compiled Hyperscan pattern
         *                                actually matched in the message text — every occurrence
         *                                across every scanned area (subject, message body, each
         *                                attachment), not just distinct areas. For example, a
         *                                pattern matching 5 separate times across the message
         *                                body records {@code 5} here. NULLABLE per the delivered
         *                                schema.
         */
        public TermDtl(String termId, String termRegexPattern, Long regexMatchHitCount) {
            this.termId = termId;
            this.termRegexPattern = termRegexPattern;
            this.regexMatchHitCount = regexMatchHitCount;
        }

        public String getTermId() {
            return termId;
        }

        public void setTermId(String termId) {
            this.termId = termId;
        }

        public String getTermRegexPattern() {
            return termRegexPattern;
        }

        public void setTermRegexPattern(String termRegexPattern) {
            this.termRegexPattern = termRegexPattern;
        }

        public Long getRegexMatchHitCount() {
            return regexMatchHitCount;
        }

        public void setRegexMatchHitCount(Long regexMatchHitCount) {
            this.regexMatchHitCount = regexMatchHitCount;
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
            return Objects.equals(regexMatchHitCount, other.regexMatchHitCount)
                    && Objects.equals(termId, other.termId)
                    && Objects.equals(termRegexPattern, other.termRegexPattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(termId, termRegexPattern, regexMatchHitCount);
        }

        @Override
        public String toString() {
            return "TermDtl[termId=" + termId + ", termRegexPattern=" + termRegexPattern
                    + ", regexMatchHitCount=" + regexMatchHitCount + "]";
        }
    }
}
