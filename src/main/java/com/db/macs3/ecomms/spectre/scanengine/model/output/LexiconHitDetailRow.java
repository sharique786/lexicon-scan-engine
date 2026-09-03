package com.db.macs3.ecomms.spectre.scanengine.model.output;

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
 * {@link EvaluatedLexicon.TermDtl#matchedText} detail for genuine hits, not
 * a broad per-group summary.
 */
public final class LexiconHitDetailRow implements Serializable {

    private final String messageId;
    private final String processId;
    private final String pipelineExecId;
    private final String datasetPartitionValue;
    private final List<EvaluatedLexicon> evaluatedLexicons;
    private final String createdBy;
    private final Instant createdTs;

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

    public String messageId() { return messageId; }
    public String processId() { return processId; }
    public String pipelineExecId() { return pipelineExecId; }
    public String datasetPartitionValue() { return datasetPartitionValue; }
    public List<EvaluatedLexicon> evaluatedLexicons() { return evaluatedLexicons; }
    public String createdBy() { return createdBy; }
    public Instant createdTs() { return createdTs; }

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

    public static final class EvaluatedLexicon implements Serializable {

        private final String id;
        private final List<TermDtl> termDtls;

        /**
         * @param id          the group's {@code feature_id}
         * @param termDtls     one entry per distinct term with a surviving match
         */
        public EvaluatedLexicon(String id, List<TermDtl> termDtls) {
            this.id = id;
            this.termDtls = termDtls;
        }

        public String id() { return id; }
        public List<TermDtl> termDtls() { return termDtls; }

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

        public static final class TermDtl implements Serializable {

            private final String termId;
            private final String matchedText;

            /**
             * @param termId          {@code <feature>::<index>}
             * @param matchedText    the serialised {@link MatchedTextJson} for this term — see that
             *                        class for the exact structure
             */
            public TermDtl(String termId, String matchedText) {
                this.termId = termId;
                this.matchedText = matchedText;
            }

            public String termId() { return termId; }
            public String matchedText() { return matchedText; }

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
