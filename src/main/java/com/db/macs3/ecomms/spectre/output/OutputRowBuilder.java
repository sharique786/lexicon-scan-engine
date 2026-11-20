package com.db.macs3.ecomms.spectre.output;

import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.model.decision.GroupEvaluationResult;
import com.db.macs3.ecomms.spectre.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.output.MatchedTextJson;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the 6 output-table rows from one message's
 * {@link MessageEvaluationResult} — see {@code DecisionTreeEvaluator} for
 * how that result is produced.
 */
public final class OutputRowBuilder {

    private static final Logger log = LoggerFactory.getLogger(OutputRowBuilder.class);
    private static final ObjectMapper MATCHED_TEXT_MAPPER = new ObjectMapper();

    /** Max hits written into one term's {@code matched_text} JSON (the true count stays in {@code regex_match_hit_count}). */
    static final int MAX_REPORTED_HITS_PER_TERM = 1_000;
    /** Max characters of any single hit's text written into {@code matched_text}. */
    static final int MAX_REPORTED_MATCH_TEXT_CHARS = 1_000;

    /**
     * {@code lexicon-hit-summary} placeholder values for an evaluated group with no matching term.
     */
    private static final String NO_HIT_TERM_ID = "N/A";
    private static final String NO_HIT_TERM_REGEX_PATTERN = "N/A";
    private static final String NO_HIT_TERM_DESCRIPTION = "N/A";

    private OutputRowBuilder() {
    }

    // ── lexicon-hit-summary ──────────────────────────────────────────────────

    /**
     * Builds the {@code lexicon-hit-summary} row for one message — one
     * {@link LexiconHitSummaryRow.EvaluatedLexicon} entry per EVALUATED
     * group (see {@link LexiconHitSummaryRow} class Javadoc for why this is
     * per-group, not per-sub-feature-member), regardless of feature type
     * (NoiseReduction/Disclaimer/Lexicon all included). A group
     * {@code DecisionTreeEvaluator} evaluated but which matched nothing is
     * included too, with {@code regexHitCount == 0} and a single placeholder
     * {@code termDtls} entry ({@code term_id}/{@code term_regex_pattern}/{@code term_description}
     * {@code "N/A"}, {@code regex_match_hit_count} 0).
     *
     * <p>Uses each group's RAW (pre-disclaimer-suppression) matches, since
     * this table is a broad "what did we hit" summary, not the alert-worthy
     * detail {@code lexicon-hit-restricted}/{@code -unrestricted} carry (see
     * {@link #buildDetailRow}, which uses the suppressed set).
     */
    public static LexiconHitSummaryRow buildSummaryRow(String messageId, String processId, String pipelineExecId,
                                                       LocalDate datasetPartitionValue,
                                                       MessageEvaluationResult evaluation,
                                                       String createdBy, Instant createdTs) {
        List<LexiconHitSummaryRow.EvaluatedLexicon> evaluatedLexicons = new ArrayList<>();

        for (GroupEvaluationResult groupResult : evaluation.getEvaluatedGroups()) {
            if (groupResult.getGroup().isMultiMember()) {
                // Composite/NoiseReduction group with several features_to_apply: one entry per member
                // lexicon, so a member with no hit is reported (zero-hit placeholder) alongside the hit ones.
                for (Map.Entry<FeatureDecisionRow, List<TermMatchResult>> entry : groupResult.getMemberMatches().entrySet()) {
                    FeatureDefinition.Body body = FeatureDefinition.parseCached(entry.getKey().getFeatureDefinitionJson()).getBody();
                    String memberName = summaryName(entry.getKey(), groupResult);
                    long memberTotal = body.getTotalTermsCount() == null ? 0 : body.getTotalTermsCount();
                    evaluatedLexicons.add(buildEvaluatedLexicon(summaryId(entry.getKey(), groupResult), memberName,
                            memberTotal, entry.getValue()));
                }
                continue;
            }
            long totalTermsCount = 0;
            List<LexiconHitSummaryRow.TermDtl> termDtls = new ArrayList<>();

            for (Map.Entry<FeatureDecisionRow, List<TermMatchResult>> entry : groupResult.getMemberMatches().entrySet()) {
                FeatureDecisionRow member = entry.getKey();
                FeatureDefinition featureDefinition = FeatureDefinition.parseCached(member.getFeatureDefinitionJson());
                Integer memberTotalTerms = featureDefinition.getBody().getTotalTermsCount();
                totalTermsCount += memberTotalTerms == null ? 0 : memberTotalTerms;

                for (TermMatchResult termMatch : entry.getValue()) {
                    // regexMatchHitCount = every individual occurrence this term's compiled pattern
                    // matched, across every scanned area (subject/body/each attachment) — the size
                    // of its AreaMatch list, NOT the distinct-term count (that is regexHitCount,
                    // below, at the EvaluatedLexicon level).
                    long regexMatchHitCount = termMatch.getMatches().size();
                    termDtls.add(new LexiconHitSummaryRow.TermDtl(termMatch.getTermId(), termMatch.getTermRegexPattern(),
                            regexMatchHitCount, termMatch.getTermDescription()));
                }
            }

            // A group this job evaluated but which matched nothing is still reported, with one
            // placeholder term_dtls entry (N/A / N/A / 0 / N/A) and regex_hit_count 0 — so a message
            // that hit nothing is distinguishable from one that was never evaluated. regex_hit_count is
            // the count of REAL matching terms, so it stays 0 here rather than termDtls.size() (1).
            long regexHitCount = termDtls.size();
            if (termDtls.isEmpty()) {
                termDtls.add(new LexiconHitSummaryRow.TermDtl(
                        NO_HIT_TERM_ID, NO_HIT_TERM_REGEX_PATTERN, 0L, NO_HIT_TERM_DESCRIPTION));
                regexHitCount = 0;
            }
            evaluatedLexicons.add(new LexiconHitSummaryRow.EvaluatedLexicon(
                    summaryId(groupResult.getMemberMatches().keySet().iterator().next(), groupResult),
                    summaryName(groupResult.getMemberMatches().keySet().iterator().next(), groupResult),
                    totalTermsCount,
                    regexHitCount,
                    termDtls));
        }

        return new LexiconHitSummaryRow(
                messageId, processId, pipelineExecId, evaluatedLexicons, datasetPartitionValue, createdBy, createdTs);
    }

    /**
     * {@code evaluated_lexicons.id}: always the member's {@code lexicon_features_to_apply_id}; the group's
     * {@code feature_id} only when the view row carries none.
     */
    private static Long summaryId(FeatureDecisionRow member, GroupEvaluationResult groupResult) {
        return member.getFeaturesToApplyId() != null ? member.getFeaturesToApplyId() : groupResult.getGroup().getFeatureId();
    }

    /**
     * {@code evaluated_lexicons.name}: always the member's {@code lexicon_features_to_apply_name}; the group's
     * {@code feature_name} only when the view row carries none.
     */
    private static String summaryName(FeatureDecisionRow member, GroupEvaluationResult groupResult) {
        String name = member.getFeaturesToApply();
        return name != null && !name.isBlank() ? name : groupResult.getGroup().getFeatureName();
    }

    private static LexiconHitSummaryRow.EvaluatedLexicon buildEvaluatedLexicon(
            Long id, String name, long totalTermsCount, List<TermMatchResult> matches) {
        List<LexiconHitSummaryRow.TermDtl> termDtls = new ArrayList<>();
        for (TermMatchResult termMatch : matches) {
            termDtls.add(new LexiconHitSummaryRow.TermDtl(termMatch.getTermId(), termMatch.getTermRegexPattern(),
                    (long) termMatch.getMatches().size(), termMatch.getTermDescription()));
        }
        long regexHitCount = termDtls.size();
        if (termDtls.isEmpty()) {
            termDtls.add(new LexiconHitSummaryRow.TermDtl(
                    NO_HIT_TERM_ID, NO_HIT_TERM_REGEX_PATTERN, 0L, NO_HIT_TERM_DESCRIPTION));
        }
        return new LexiconHitSummaryRow.EvaluatedLexicon(id, name, totalTermsCount, regexHitCount, termDtls);
    }

    // ── lexicon-hit-restricted / lexicon-hit-unrestricted (shared shape) ────

    /**
     * Builds the {@code lexicon-hit-restricted}/{@code -unrestricted} row
     * for one message from three sources: the disclaimer-SUPPRESSED Lexicon
     * match set (see {@link MessageEvaluationResult finalLexiconMatchesByFeatureId()}),
     * plus the matches of any Disclaimer group that was a hit (reported in full —
     * a disclaimer match is what suppresses an overlapping Lexicon match, so it is
     * the entry that remains where the two collide), plus the matches of any
     * NoiseReduction group that was a hit. A NoiseReduction hit is necessarily the
     * message's LAST evaluated group — it short-circuits the decision tree, so no
     * Disclaimer or Lexicon group ran and there is no suppression to apply. A
     * NoiseReduction or Disclaimer group that was evaluated but not a hit (including
     * an AND group with only some members matching) is not reported here.
     *
     * @return null when there is nothing to report — no NoiseReduction or Disclaimer
     * group was a hit, and every Lexicon-category group had zero surviving matches
     * after suppression. This table carries genuine hit detail, not a broad per-message
     * summary the way {@code lexicon-hit-summary} is, so a message with nothing to
     * report simply has no row here — the caller should skip writing
     * when this returns null, not write a row with an empty
     * {@code evaluated_lexicons} array.
     */
    public static LexiconHitDetailRow buildDetailRow(String messageId, String processId, String pipelineExecId,
                                                     LocalDate datasetPartitionValue,
                                                     MessageEvaluationResult evaluation,
                                                     String createdBy, Instant createdTs) {
        return buildDetailRow(messageId, processId, pipelineExecId, datasetPartitionValue, evaluation,
                createdBy, createdTs, Map.of());
    }

    /**
     * @param attachmentNames attachment_id → {@code attachments.metadata.file_name}, used for each
     *                        {@code attachment_text[].attachment_name}; an id with no entry (or a null
     *                        file name) is written as {@code ""}
     */
    public static LexiconHitDetailRow buildDetailRow(String messageId, String processId, String pipelineExecId,
                                                     LocalDate datasetPartitionValue,
                                                     MessageEvaluationResult evaluation,
                                                     String createdBy, Instant createdTs,
                                                     Map<String, String> attachmentNames) {
        // Keyed by lexicon_features_to_apply_id (see summaryId), not the group's feature_id.
        Map<Long, List<TermMatchResult>> matchesByFeatureId = new LinkedHashMap<>();
        for (GroupEvaluationResult groupResult : evaluation.getEvaluatedGroups()) {
            // NoiseReduction/Disclaimer are reported only when the group is a hit; Lexicon matches are reported
            // in full, INCLUDING ones a disclaimer overlaps (suppression only affects feature-hit-summary).
            boolean lexicon = !groupResult.getGroup().isNoiseReduction() && !groupResult.getGroup().isDisclaimer();
            if (lexicon || groupResult.isHit()) {
                for (Map.Entry<FeatureDecisionRow, List<TermMatchResult>> member : groupResult.getMemberMatches().entrySet()) {
                    if (!member.getValue().isEmpty()) {
                        matchesByFeatureId.computeIfAbsent(summaryId(member.getKey(), groupResult), k -> new ArrayList<>())
                                .addAll(member.getValue());
                    }
                }
            }
        }
        if (matchesByFeatureId.isEmpty()) {
            return null;
        }

        List<LexiconHitDetailRow.EvaluatedLexicon> evaluatedLexicons = new ArrayList<>();
        for (Map.Entry<Long, List<TermMatchResult>> entry : matchesByFeatureId.entrySet()) {
            List<LexiconHitDetailRow.EvaluatedLexicon.TermDtl> termDtls = new ArrayList<>();
            for (TermMatchResult termMatch : entry.getValue()) {
                String matchedTextJson = buildMatchedTextJson(
                        messageId, termMatch.getTermId(), termMatch.getMatches(), attachmentNames);
                termDtls.add(new LexiconHitDetailRow.EvaluatedLexicon.TermDtl(termMatch.getTermId(), matchedTextJson));
            }
            evaluatedLexicons.add(new LexiconHitDetailRow.EvaluatedLexicon(entry.getKey(), termDtls));
        }

        return new LexiconHitDetailRow(
                messageId, processId, pipelineExecId, evaluatedLexicons, datasetPartitionValue, createdBy, createdTs);
    }

    /**
     * Builds the {@code matched_text} JSON for one term's matches — see
     * {@link MatchedTextJson} for the exact structure.
     */
    static String buildMatchedTextJson(String messageId, List<AreaMatch> matches, Map<String, String> attachmentNames) {
        return buildMatchedTextJson(messageId, "unknown", matches, attachmentNames);
    }

    static String buildMatchedTextJson(String messageId, String termId, List<AreaMatch> matches,
                                       Map<String, String> attachmentNames) {
        warnIfExcessive(messageId, termId, matches);

        List<MatchedTextJson.TextHit> msgText = new ArrayList<>();
        List<MatchedTextJson.TextHit> subject = new ArrayList<>();
        Map<String, List<MatchedTextJson.TextHit>> attachmentHits = new LinkedHashMap<>();

        // Bound the JSON: a short/common pattern over a multi-MB attachment can match hundreds of
        // thousands of times, and each hit's text is serialised into ONE String per term.
        int reportable = Math.min(matches.size(), MAX_REPORTED_HITS_PER_TERM);
        for (AreaMatch areaMatch : matches.subList(0, reportable)) {
            MatchedTextJson.TextHit hit = toTextHit(areaMatch);
            switch (areaMatch.getArea()) {
                case MESSAGE_BODY -> msgText.add(hit);
                case SUBJECT -> subject.add(hit);
                case ATTACHMENT ->
                        attachmentHits.computeIfAbsent(areaMatch.getAttachmentId(), unusedKey -> new ArrayList<>()).add(hit);
                default -> throw new IllegalArgumentException("Unexpected area: " + areaMatch.getArea());
            }
        }

        MatchedTextJson.HitDetail detail = new MatchedTextJson.HitDetail(
                messageId, msgText, subject, toAttachmentHits(attachmentHits, attachmentNames));
        return serializeMatchedText(messageId, new MatchedTextJson(List.of(detail)));
    }

    private static void warnIfExcessive(String messageId, String termId, List<AreaMatch> matches) {
        if (matches.size() <= MAX_REPORTED_HITS_PER_TERM) {
            return;
        }
        long totalChars = 0;
        int longestHit = 0;
        for (AreaMatch m : matches) {
            String t = m.getSpan().getMatchedText();
            int len = t == null ? 0 : t.length();
            totalChars += len;
            longestHit = Math.max(longestHit, len);
        }
        log.warn("Excessive matches for messageId={} termId={}: {} hits ({} matched chars, longest hit {} chars) — "
                        + "matched_text JSON capped to {} hits of at most {} chars each",
                messageId, termId, matches.size(), totalChars, longestHit,
                MAX_REPORTED_HITS_PER_TERM, MAX_REPORTED_MATCH_TEXT_CHARS);
    }

    private static MatchedTextJson.TextHit toTextHit(AreaMatch areaMatch) {
        String text = areaMatch.getSpan().getMatchedText();
        if (text != null && text.length() > MAX_REPORTED_MATCH_TEXT_CHARS) {
            text = text.substring(0, MAX_REPORTED_MATCH_TEXT_CHARS);
        }
        return new MatchedTextJson.TextHit(
                text, areaMatch.getSpan().getStartCharIndex(), areaMatch.getSpan().length());
    }

    private static List<MatchedTextJson.AttachmentTextHit> toAttachmentHits(
            Map<String, List<MatchedTextJson.TextHit>> attachmentHits, Map<String, String> attachmentNames) {
        List<MatchedTextJson.AttachmentTextHit> attachmentText = new ArrayList<>();
        for (Map.Entry<String, List<MatchedTextJson.TextHit>> attachmentEntry : attachmentHits.entrySet()) {
            String attachmentName = attachmentNames.get(attachmentEntry.getKey());
            attachmentText.add(new MatchedTextJson.AttachmentTextHit(
                    attachmentEntry.getKey(), attachmentName == null ? "" : attachmentName, attachmentEntry.getValue()));
        }
        return attachmentText;
    }

    private static String serializeMatchedText(String messageId, MatchedTextJson wrapper) {
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
     * @param featureTaggingType carried verbatim from the view's {@code feature_tagging_type}
     *                           column — taken from the first evaluated group's first member,
     *                           since it is a per-message (not per-group) property in practice
     */
    public static FeatureHitSummaryRow buildFeatureHitSummaryRow(String messageId, LocalDate datasetPartitionValue,
                                                                 String pipelineExecId, String processId,
                                                                 String featureTaggingType,
                                                                 MessageEvaluationResult evaluation,
                                                                 String createdBy, Instant createdTs) {
        List<FeatureHitSummaryRow.Feature> features = new ArrayList<>();
        for (GroupEvaluationResult groupResult : evaluation.getEvaluatedGroups()) {
            FeatureGroup group = groupResult.getGroup();
            features.add(new FeatureHitSummaryRow.Feature(
                    group.getFeatureId(),
                    group.getFeatureName(),
                    group.getFeatureType(),
                    group.isNoiseReduction(),
                    groupResult.isHit() && (!isStandardLexicon(group)
                            || survives(evaluation, group, flattenMatches(groupResult))),
                    buildSubFeatures(evaluation, groupResult)));
        }

        // FeatureHitSummaryRow's constructor argument order matches the delivered schema's field
        // order, which does NOT match this method's own parameter order — see that class's Javadoc.
        return new FeatureHitSummaryRow(
                messageId, features, datasetPartitionValue, featureTaggingType,
                createdBy, createdTs, processId, pipelineExecId);
    }

    private static List<FeatureHitSummaryRow.SubFeature> buildSubFeatures(MessageEvaluationResult evaluation,
                                                                          GroupEvaluationResult groupResult) {
        FeatureGroup group = groupResult.getGroup();
        // NoiseReduction and composite groups always report their members as sub_features, even a
        // single-member one; only a plain lexicon/disclaimer group with one row has none.
        if (!listsMembersAsSubFeatures(group)) {
            return new ArrayList<>();
        }
        List<FeatureHitSummaryRow.SubFeature> subFeatures = new ArrayList<>();
        for (Map.Entry<FeatureDecisionRow, Boolean> entry : groupResult.getMemberHit().entrySet()) {
            FeatureDecisionRow member = entry.getKey();
            String subType = member.getSubFeatureType() == null || member.getSubFeatureType().isBlank()
                    ? BqColumns.FeatureType.LEXICON : member.getSubFeatureType();
            boolean memberHit = entry.getValue() && (!isStandardLexicon(group)
                    || survives(evaluation, group, groupResult.getMemberMatches().get(member)));
            subFeatures.add(new FeatureHitSummaryRow.SubFeature(subType, member.getFeaturesToApply(), memberHit));
        }
        return subFeatures;
    }

    private static boolean listsMembersAsSubFeatures(FeatureGroup group) {
        return group.isMultiMember() || group.isNoiseReduction()
                || BqColumns.FeatureType.COMPOSITE.equalsIgnoreCase(group.getFeatureType());
    }

    private static boolean isStandardLexicon(FeatureGroup group) {
        return !group.isNoiseReduction() && !group.isDisclaimer();
    }

    /** True iff at least one of these matches' terms still has a match after disclaimer suppression. */
    private static boolean survives(MessageEvaluationResult evaluation, FeatureGroup group, List<TermMatchResult> matches) {
        List<TermMatchResult> kept = evaluation.getFinalLexiconMatchesByFeatureId().get(group.getFeatureId());
        if (kept == null || matches == null) {
            return false;
        }
        for (TermMatchResult m : matches) {
            for (TermMatchResult k : kept) {
                if (k.getTermId().equals(m.getTermId())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<TermMatchResult> flattenMatches(GroupEvaluationResult groupResult) {
        List<TermMatchResult> all = new ArrayList<>();
        groupResult.getMemberMatches().values().forEach(all::addAll);
        return all;
    }
}
