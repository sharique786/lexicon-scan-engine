package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One row of {@code lexicon-hit-summary} — per-message summary of every
 * evaluated feature (regardless of type: standard Lexicon, Disclaimer, or
 * NoiseReduction) and which of its terms matched.
 *
 * <h2>One entry per evaluated FEATURE GROUP, not per sub-feature member</h2>
 * <p>{@link EvaluatedLexicon#id}/{@link EvaluatedLexicon#name} are the
 * view's {@code feature_id}/{@code feature_name} (confirmed) — which are
 * shared across every sub-feature member of a composite/NoiseReduction
 * group. One {@link EvaluatedLexicon} entry is therefore built per
 * EVALUATED GROUP (see {@code DecisionTreeEvaluator}'s
 * {@code GroupEvaluationResult}), aggregating every member's own matched
 * terms into its {@link EvaluatedLexicon#termDtls} — each {@code term_id}
 * still identifies exactly which underlying lexicon (which member's
 * {@code body.feature}) it came from, since {@code term_id} is
 * {@code <feature>::<index>}.
 *
 * <p>Only groups that were ACTUALLY evaluated appear — a NoiseReduction
 * group short-circuit means every later group (further NoiseReduction, the
 * Disclaimer group, all Lexicon groups) simply has no entry at all here,
 * since {@code DecisionTreeEvaluator} never evaluated them.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class LexiconHitSummaryRow implements Serializable {

    private final String messageId;
    private final String processId;
    private final String pipelineExecId;
    private final List<EvaluatedLexicon> evaluatedLexicons;
    private final String createdBy;
    private final Instant createdTs;

    /**
     * @param messageId          the message this summary is for
     * @param processId            the process run this row belongs to
     * @param pipelineExecId       the pipeline execution this row belongs to
     * @param evaluatedLexicons    one entry per evaluated feature group
     * @param createdBy             the writing job's identity
     * @param createdTs              write time, UTC
     */
    public LexiconHitSummaryRow(String messageId, String processId, String pipelineExecId,
                                 List<EvaluatedLexicon> evaluatedLexicons, String createdBy, Instant createdTs) {
        this.messageId = messageId;
        this.processId = processId;
        this.pipelineExecId = pipelineExecId;
        this.evaluatedLexicons = evaluatedLexicons;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
    }

    public String messageId() { return messageId; }
    public String processId() { return processId; }
    public String pipelineExecId() { return pipelineExecId; }
    public List<EvaluatedLexicon> evaluatedLexicons() { return evaluatedLexicons; }
    public String createdBy() { return createdBy; }
    public Instant createdTs() { return createdTs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LexiconHitSummaryRow)) return false;
        LexiconHitSummaryRow other = (LexiconHitSummaryRow) o;
        return Objects.equals(messageId, other.messageId)
                && Objects.equals(processId, other.processId)
                && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(evaluatedLexicons, other.evaluatedLexicons)
                && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, processId, pipelineExecId, evaluatedLexicons, createdBy, createdTs);
    }

    @Override
    public String toString() {
        return "LexiconHitSummaryRow[messageId=" + messageId + ", processId=" + processId
                + ", pipelineExecId=" + pipelineExecId + ", evaluatedLexicons=" + evaluatedLexicons
                + ", createdBy=" + createdBy + ", createdTs=" + createdTs + "]";
    }

    public static final class EvaluatedLexicon implements Serializable {

        private final String id;
        private final String name;
        private final long totalTermsCount;
        private final long regexHitCount;
        private final List<TermDtl> termDtls;

        /**
         * @param id                 the group's {@code feature_id}
         * @param name                the group's {@code feature_name}
         * @param totalTermsCount    sum of every member's {@code feature_definition.body.totalTermsCount}
         * @param regexHitCount       count of DISTINCT {@code term_id}s that matched across every
         *                             member of this group (the length of {@link #termDtls})
         * @param termDtls             one entry per distinct term that matched, across every member
         */
        public EvaluatedLexicon(String id, String name, long totalTermsCount, long regexHitCount,
                                 List<TermDtl> termDtls) {
            this.id = id;
            this.name = name;
            this.totalTermsCount = totalTermsCount;
            this.regexHitCount = regexHitCount;
            this.termDtls = termDtls;
        }

        public String id() { return id; }
        public String name() { return name; }
        public long totalTermsCount() { return totalTermsCount; }
        public long regexHitCount() { return regexHitCount; }
        public List<TermDtl> termDtls() { return termDtls; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EvaluatedLexicon)) return false;
            EvaluatedLexicon other = (EvaluatedLexicon) o;
            return totalTermsCount == other.totalTermsCount
                    && regexHitCount == other.regexHitCount
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

    public static final class TermDtl implements Serializable {

        private final String termId;
        private final String termRegexPattern;
        private final long regexMatchHitCount;

        /**
         * @param termId                  {@code <feature>::<index>} — see {@code TermIdBuilder}
         * @param termRegexPattern       the compiled Hyperscan pattern text, for auditability
         * @param regexMatchHitCount     how many times this term's compiled Hyperscan pattern
         *                                actually matched in the message text — every occurrence
         *                                across every scanned area (subject, message body, each
         *                                attachment), not just distinct areas. For example, a
         *                                pattern matching 5 separate times across the message
         *                                body records {@code 5} here.
         */
        public TermDtl(String termId, String termRegexPattern, long regexMatchHitCount) {
            this.termId = termId;
            this.termRegexPattern = termRegexPattern;
            this.regexMatchHitCount = regexMatchHitCount;
        }

        public String termId() { return termId; }
        public String termRegexPattern() { return termRegexPattern; }
        public long regexMatchHitCount() { return regexMatchHitCount; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TermDtl)) return false;
            TermDtl other = (TermDtl) o;
            return regexMatchHitCount == other.regexMatchHitCount
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
