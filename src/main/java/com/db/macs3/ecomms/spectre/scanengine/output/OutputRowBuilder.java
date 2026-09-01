package com.db.macs3.ecomms.spectre.scanengine.output;

import com.db.macs3.ecomms.spectre.scanengine.model.decision.GroupEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitSummaryRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.MatchedTextJson;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the 6 output-table rows from one message's
 * {@link MessageEvaluationResult} — see {@code DecisionTreeEvaluator} for
 * how that result is produced.
 */
public final class OutputRowBuilder {

    private static final ObjectMapper MATCHED_TEXT_MAPPER = new ObjectMapper();

    private OutputRowBuilder() {}

    // ── lexicon-hit-summary ──────────────────────────────────────────────────

    /**
     * Builds the {@code lexicon-hit-summary} row for one message — one
     * {@link LexiconHitSummaryRow.EvaluatedLexicon} entry per EVALUATED
     * group (see {@link LexiconHitSummaryRow} class Javadoc for why this is
     * per-group, not per-sub-feature-member), regardless of feature type
     * (NoiseReduction/Disclaimer/Lexicon all included) — reflecting
     * everything {@code DecisionTreeEvaluator} actually evaluated for this
     * message, stopping wherever it stopped.
     *
     * <p>Uses each group's RAW (pre-disclaimer-suppression) matches, since
     * this table is a broad "what did we check and find" summary, not the
     * alert-worthy detail {@code lexicon-hit-restricted}/{@code -unrestricted}
     * carry (see {@link #buildDetailRow}, which uses the suppressed set).
     */
    public static LexiconHitSummaryRow buildSummaryRow(String messageId, String processId, String pipelineExecId,
                                                         MessageEvaluationResult evaluation,
                                                         String createdBy, Instant createdTs) {
        List<LexiconHitSummaryRow.EvaluatedLexicon> evaluatedLexicons = new ArrayList<>();

        for (GroupEvaluationResult groupResult : evaluation.evaluatedGroups()) {
            long totalTermsCount = 0;
            List<LexiconHitSummaryRow.TermDtl> termDtls = new ArrayList<>();

            for (Map.Entry<FeatureDecisionRow, List<TermMatchResult>> entry : groupResult.memberMatches().entrySet()) {
                FeatureDecisionRow member = entry.getKey();
                FeatureDefinition featureDefinition = FeatureDefinition.parse(member.featureDefinitionJson());
                Integer memberTotalTerms = featureDefinition.body().totalTermsCount();
                totalTermsCount += memberTotalTerms == null ? 0 : memberTotalTerms;

                for (TermMatchResult termMatch : entry.getValue()) {
                    // regexMatchHitCount = every individual occurrence this term's compiled pattern
                    // matched, across every scanned area (subject/body/each attachment) — the size
                    // of its AreaMatch list, NOT the distinct-term count (that is regexHitCount,
                    // below, at the EvaluatedLexicon level).
                    long regexMatchHitCount = termMatch.matches().size();
                    termDtls.add(new LexiconHitSummaryRow.TermDtl(
                            termMatch.termId(), termMatch.termRegexPattern(), regexMatchHitCount));
                }
            }

            evaluatedLexicons.add(new LexiconHitSummaryRow.EvaluatedLexicon(
                    groupResult.group().featureId(),
                    groupResult.group().featureName(),
                    totalTermsCount,
                    termDtls.size(),
                    termDtls));
        }

        return new LexiconHitSummaryRow(messageId, processId, pipelineExecId, evaluatedLexicons, createdBy, createdTs);
    }

    // ── lexicon-hit-restricted / lexicon-hit-unrestricted (shared shape) ────

    /**
     * Builds the {@code lexicon-hit-restricted}/{@code -unrestricted} row
     * for one message, using the disclaimer-SUPPRESSED match set (see
     * {@link MessageEvaluationResult#finalLexiconMatchesByFeatureId()}).
     *
     * @return null when there is nothing to report — the message was
     *          short-circuited by noise reduction, or every Lexicon-category
     *          group had zero surviving matches after suppression. This table
     *          carries genuine hit detail, not a broad per-message summary the
     *          way {@code lexicon-hit-summary} is, so a message with nothing to
     *          report simply has no row here — the caller should skip writing
     *          when this returns null, not write a row with an empty
     *          {@code evaluated_lexicons} array.
     */
    public static LexiconHitDetailRow buildDetailRow(String messageId, String processId, String pipelineExecId,
                                                       String datasetPartitionValue,
                                                       MessageEvaluationResult evaluation,
                                                       String createdBy, Instant createdTs) {
        if (evaluation.finalLexiconMatchesByFeatureId().isEmpty()) {
            return null;
        }

        List<LexiconHitDetailRow.EvaluatedLexicon> evaluatedLexicons = new ArrayList<>();
        for (Map.Entry<String, List<TermMatchResult>> entry : evaluation.finalLexiconMatchesByFeatureId().entrySet()) {
            List<LexiconHitDetailRow.EvaluatedLexicon.TermDtl> termDtls = new ArrayList<>();
            for (TermMatchResult termMatch : entry.getValue()) {
                String matchedTextJson = buildMatchedTextJson(messageId, termMatch.matches());
                termDtls.add(new LexiconHitDetailRow.EvaluatedLexicon.TermDtl(termMatch.termId(), matchedTextJson));
            }
            evaluatedLexicons.add(new LexiconHitDetailRow.EvaluatedLexicon(entry.getKey(), termDtls));
        }

        return new LexiconHitDetailRow(
                messageId, processId, pipelineExecId, datasetPartitionValue, evaluatedLexicons, createdBy, createdTs);
    }

    /**
     * Builds the {@code matched_text} JSON for one term's matches — see
     * {@link MatchedTextJson} for the exact structure.
     */
    static String buildMatchedTextJson(String messageId, List<AreaMatch> matches) {
        List<MatchedTextJson.TextHit> msgText = new ArrayList<>();
        List<MatchedTextJson.TextHit> subject = new ArrayList<>();
        Map<String, List<MatchedTextJson.TextHit>> attachmentHits = new LinkedHashMap<>();

        for (AreaMatch areaMatch : matches) {
            MatchedTextJson.TextHit hit = new MatchedTextJson.TextHit(
                    areaMatch.span().matchedText(), areaMatch.span().startCharIndex(), areaMatch.span().length());
            switch (areaMatch.area()) {
                case MESSAGE_BODY -> msgText.add(hit);
                case SUBJECT -> subject.add(hit);
                case ATTACHMENT -> attachmentHits.computeIfAbsent(areaMatch.attachmentId(), unusedKey -> new ArrayList<>()).add(hit);
            }
        }

        List<MatchedTextJson.AttachmentTextHit> attachmentText = new ArrayList<>();
        for (Map.Entry<String, List<MatchedTextJson.TextHit>> attachmentEntry : attachmentHits.entrySet()) {
            attachmentText.add(new MatchedTextJson.AttachmentTextHit(attachmentEntry.getKey(), attachmentEntry.getValue()));
        }

        MatchedTextJson.HitDetail detail = new MatchedTextJson.HitDetail(messageId, msgText, subject, attachmentText);
        MatchedTextJson wrapper = new MatchedTextJson(List.of(detail));

        try {
            return MATCHED_TEXT_MAPPER.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            // MatchedTextJson is a closed set of plain, Jackson-friendly records/classes with no
            // cyclic references — serialization failure here would indicate a structural bug in
            // this class, not a data-dependent condition callers could meaningfully recover from.
            throw new IllegalStateException(
                    "Failed to serialize matched_text JSON for messageId=" + messageId + ": " + e.getMessage(), e);
        }
    }

    // ── feature-hit-summary ──────────────────────────────────────────────────

    /**
     * Builds the {@code feature-hit-summary} row for one message — one
     * {@link FeatureHitSummaryRow.Feature} entry per evaluated group, each
     * with its own resolved {@code hitStatus} and, for multi-member groups,
     * a {@link FeatureHitSummaryRow.SubFeature} entry per member.
     *
     * @param featureTaggingType    carried verbatim from the view's {@code feature_tagging_type}
     *                              column — taken from the first evaluated group's first member,
     *                              since it is a per-message (not per-group) property in practice
     */
    public static FeatureHitSummaryRow buildFeatureHitSummaryRow(String messageId, String datasetPartitionValue,
                                                                   String pipelineExecId, String processId,
                                                                   String featureTaggingType,
                                                                   MessageEvaluationResult evaluation,
                                                                   String createdBy, Instant createdTs) {
        List<FeatureHitSummaryRow.Feature> features = new ArrayList<>();

        for (GroupEvaluationResult groupResult : evaluation.evaluatedGroups()) {
            List<FeatureHitSummaryRow.SubFeature> subFeatures = new ArrayList<>();
            if (groupResult.group().isMultiMember()) {
                for (Map.Entry<FeatureDecisionRow, Boolean> entry : groupResult.memberHit().entrySet()) {
                    FeatureDecisionRow member = entry.getKey();
                    subFeatures.add(new FeatureHitSummaryRow.SubFeature(
                            member.subFeatureType(), member.featuresToApply(), entry.getValue()));
                }
            }

            features.add(new FeatureHitSummaryRow.Feature(
                    parseFeatureId(groupResult.group().featureId()),
                    groupResult.group().featureName(),
                    groupResult.group().featureType(),
                    groupResult.group().isNoiseReduction(),
                    groupResult.isHit(),
                    subFeatures));
        }

        return new FeatureHitSummaryRow(
                messageId, datasetPartitionValue, pipelineExecId, processId, featureTaggingType,
                features, createdBy, createdTs);
    }

    private static long parseFeatureId(String featureId) {
        try {
            return Long.parseLong(featureId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "featureId '" + featureId + "' is not a valid integer — feature-hit-summary.features.id "
                    + "requires an INTEGER-typed feature_id from the view.", e);
        }
    }
}
