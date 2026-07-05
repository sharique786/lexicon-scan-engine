package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Core Spark executor logic — processes one partition of (message + grouped
 * feature decisions) rows, producing a {@link MessageScanResult} per message
 * that feeds all three output tables from a single scan pass.
 *
 * <h2>Per-message algorithm</h2>
 * <ol>
 *   <li>Separate the message's {@link FeatureDecisionRow}s into noise-reduction
 *       vs standard groups (grouping composite sub-features under their parent id)</li>
 *   <li>Scan every DISTINCT lexicon name needed by the noise-reduction group</li>
 *   <li>Apply {@link NoiseReductionEvaluator} to decide whether to skip standard features</li>
 *   <li>If not skipped, scan every DISTINCT lexicon name needed by the standard group</li>
 *   <li>Build one {@link FeatureHitSummaryRow} per evaluated (message, feature[, sub-feature])
 *       combination, Yes/No based on whether that specific lexicon had a hit</li>
 *   <li>Build one {@link EvaluatedLexicon} per lexicon name that had at least one hit
 *       (lexicons with zero hits are omitted from {@code evaluated_lexicons} — see
 *       {@link LexiconHitSummaryRow})</li>
 *   <li>Apply restricted-message masking: if {@code message_type='restricted'} and
 *       there was at least one hit, redact {@code lexicon-hit-summary}'s matched_text
 *       and additionally emit a {@link LexiconHitRestrictedRow} carrying the real text</li>
 * </ol>
 */
public class LexiconScanPartitionFunction implements MapPartitionsFunction<Row, MessageScanResult> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(LexiconScanPartitionFunction.class);

    private final Broadcast<Map<String, byte[]>> broadcastHdbBytes;
    private final Broadcast<Map<String, Map<Integer, TermManifestEntry>>> broadcastManifests;
    private final String pipelineExecId;

    private final HyperscanMatcher        matcher;
    private final NoiseReductionEvaluator noiseEvaluator;
    private final ObjectMapper            objectMapper;

    public LexiconScanPartitionFunction(Broadcast<Map<String, byte[]>> broadcastHdbBytes,
                                         Broadcast<Map<String, Map<Integer, TermManifestEntry>>> broadcastManifests,
                                         String pipelineExecId) {
        this.broadcastHdbBytes  = broadcastHdbBytes;
        this.broadcastManifests = broadcastManifests;
        this.pipelineExecId     = pipelineExecId;
        this.matcher            = new HyperscanMatcher();
        this.noiseEvaluator     = new NoiseReductionEvaluator();
        this.objectMapper       = new ObjectMapper();
    }

    @Override
    public Iterator<MessageScanResult> call(Iterator<Row> partition) {
        List<MessageScanResult> results = new ArrayList<>();
        int messageCount = 0;

        while (partition.hasNext()) {
            Row row = partition.next();
            messageCount++;
            try {
                results.add(processMessage(row));
            } catch (Exception e) {
                String msgId = getStringField(row, "message_id");
                log.error("Failed to process message '{}': {}", msgId, e.getMessage(), e);
            }
        }

        log.info("Partition done: {} messages processed", messageCount);
        return results.iterator();
    }

    // ── Message processing ────────────────────────────────────────────────────

    private MessageScanResult processMessage(Row row) {
        String messageId       = getStringField(row, "message_id");
        String runDate         = getStringField(row, "run_date");
        String processId       = getStringField(row, "process_id");
        String sentDate        = getStringField(row, "sent_date");
        String messageType     = getStringField(row, "message_type");
        String contentRawText  = getStringField(row, "content_raw_text");
        List<String> attachmentTexts = extractAttachmentTexts(row);

        boolean isRestricted = "restricted".equalsIgnoreCase(messageType);

        List<FeatureDecisionRow> allFeatures = extractFeatures(row);
        if (allFeatures.isEmpty()) {
            log.debug("Message '{}' has no applicable lexicon/composite features — skipping", messageId);
            return emptyResult(messageId, runDate, processId, sentDate, messageType);
        }

        List<FeatureDecisionRow> noiseFeatures = allFeatures.stream()
                .filter(FeatureDecisionRow::isNoiseReduction)
                .collect(Collectors.toList());
        List<FeatureDecisionRow> stdFeatures = allFeatures.stream()
                .filter(f -> !f.isNoiseReduction())
                .collect(Collectors.toList());

        Map<String, Map<String, List<TermMatch>>> allScanResults = new HashMap<>();
        List<FeatureHitSummaryRow> featureHitRows = new ArrayList<>();
        boolean skipStandard = false;

        // ── Noise-reduction features (always evaluated first) ──────────────────
        if (!noiseFeatures.isEmpty()) {
            Map<String, Map<String, List<TermMatch>>> nrResults = scanDistinctLexicons(noiseFeatures, contentRawText, attachmentTexts);
            allScanResults.putAll(nrResults);

            skipStandard = noiseEvaluator.shouldSkipStandardFeatures(noiseFeatures, flattenForEvaluator(nrResults));

            for (FeatureDecisionRow f : noiseFeatures) {
                featureHitRows.add(buildFeatureHitRow(f, messageId, runDate, processId, sentDate,
                                                       messageType, nrResults));
            }

            if (skipStandard) {
                log.debug("Message '{}': noise reduction suppressed standard features (op={})",
                          messageId, noiseEvaluator.resolveOperator(noiseFeatures));
            }
        }

        // ── Standard features (only if not suppressed) ─────────────────────────
        if (!stdFeatures.isEmpty() && !skipStandard) {
            Map<String, Map<String, List<TermMatch>>> stdResults = scanDistinctLexicons(stdFeatures, contentRawText, attachmentTexts);
            allScanResults.putAll(stdResults);

            for (FeatureDecisionRow f : stdFeatures) {
                featureHitRows.add(buildFeatureHitRow(f, messageId, runDate, processId, sentDate,
                                                       messageType, stdResults));
            }
        }

        // ── Build evaluated_lexicons (only entries with hits) ───────────────────
        List<EvaluatedLexicon> evaluatedLexicons = buildEvaluatedLexicons(allFeatures, allScanResults);

        // ── Assemble lexicon-hit-summary row, with restricted-message masking ──
        LexiconHitSummaryRow summaryRow = new LexiconHitSummaryRow();
        summaryRow.setMessageId(messageId);
        summaryRow.setRunDate(runDate);
        summaryRow.setProcessId(processId);
        summaryRow.setPipelineExecId(pipelineExecId);
        summaryRow.setSentDate(sentDate);
        summaryRow.setMessageType(messageType);
        summaryRow.setCreatedTs(Timestamp.from(Instant.now()));

        LexiconHitRestrictedRow restrictedRow = null;
        boolean hasAnyHit = evaluatedLexicons.stream().anyMatch(EvaluatedLexicon::hasHit);

        if (isRestricted && hasAnyHit) {
            // Real (unredacted) data goes to the restricted table...
            restrictedRow = buildRestrictedRow(messageId, processId, evaluatedLexicons);
            // ...and lexicon-hit-summary gets the redacted placeholder instead.
            List<EvaluatedLexicon> redacted = evaluatedLexicons.stream()
                    .map(EvaluatedLexicon::withRedactedTermDtls)
                    .collect(Collectors.toList());
            summaryRow.setEvaluatedLexicons(redacted);
        } else {
            summaryRow.setEvaluatedLexicons(evaluatedLexicons);
        }

        return MessageScanResult.of(summaryRow, restrictedRow, featureHitRows);
    }

    private MessageScanResult emptyResult(String messageId, String runDate, String processId,
                                           String sentDate, String messageType) {
        LexiconHitSummaryRow row = new LexiconHitSummaryRow();
        row.setMessageId(messageId);
        row.setRunDate(runDate);
        row.setProcessId(processId);
        row.setPipelineExecId(pipelineExecId);
        row.setSentDate(sentDate);
        row.setMessageType(messageType);
        row.setCreatedTs(Timestamp.from(Instant.now()));
        return MessageScanResult.of(row, null, List.of());
    }

    // ── Scanning ──────────────────────────────────────────────────────────────

    /** Segment label for the message body, used as a map key throughout. */
    private static final String SEGMENT_MSG = "msg";

    /**
     * Adapts the segment-aware scan result shape to the flat
     * {@code Map<lexiconName, List<TermMatch>>} contract expected by
     * {@link NoiseReductionEvaluator}, which only needs to know WHETHER a
     * lexicon matched at all — segment attribution is irrelevant for the
     * noise-reduction decision itself.
     */
    private Map<String, List<TermMatch>> flattenForEvaluator(Map<String, Map<String, List<TermMatch>>> bySegment) {
        Map<String, List<TermMatch>> flat = new HashMap<>();
        for (Map.Entry<String, Map<String, List<TermMatch>>> entry : bySegment.entrySet()) {
            List<TermMatch> all = new ArrayList<>();
            entry.getValue().values().forEach(all::addAll);
            flat.put(entry.getKey(), all);
        }
        return flat;
    }

    /**
     * Scans every DISTINCT lexicon name referenced by {@code features} against
     * the message body and each attachment, returning a map of
     * lexiconName -> (segment -> matches), where segment is {@code "msg"} for
     * the message body or {@code "attachment-N"} for the Nth attachment (0-based).
     * Segment attribution is preserved end-to-end so {@code matched_text} JSON
     * can correctly report which segment each match came from.
     */
    private Map<String, Map<String, List<TermMatch>>> scanDistinctLexicons(List<FeatureDecisionRow> features,
                                                                            String contentRawText,
                                                                            List<String> attachmentTexts) {
        Map<String, Map<String, List<TermMatch>>> results = new HashMap<>();
        Set<String> distinctLexiconNames = features.stream()
                .map(FeatureDecisionRow::getLexiconName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        for (String lexiconName : distinctLexiconNames) {
            Map<String, List<TermMatch>> bySegment = new LinkedHashMap<>();

            if (contentRawText != null && !contentRawText.isBlank()) {
                List<TermMatch> bodyMatches = matcher.scan(lexiconName, contentRawText, broadcastHdbBytes, broadcastManifests);
                if (!bodyMatches.isEmpty()) bySegment.put(SEGMENT_MSG, bodyMatches);
            }
            for (int i = 0; i < attachmentTexts.size(); i++) {
                String attText = attachmentTexts.get(i);
                if (attText != null && !attText.isBlank()) {
                    List<TermMatch> attMatches = matcher.scan(lexiconName, attText, broadcastHdbBytes, broadcastManifests);
                    if (!attMatches.isEmpty()) bySegment.put("attachment-" + i, attMatches);
                }
            }
            results.put(lexiconName, bySegment);
        }
        return results;
    }

    /** @return {@code true} if any segment for this lexicon has at least one match. */
    private boolean hasAnyMatch(Map<String, List<TermMatch>> bySegment) {
        return bySegment != null && bySegment.values().stream().anyMatch(l -> l != null && !l.isEmpty());
    }

    // ── feature-hit-summary construction ────────────────────────────────────────

    private FeatureHitSummaryRow buildFeatureHitRow(FeatureDecisionRow f, String messageId, String runDate,
                                                     String processId, String sentDate, String messageType,
                                                     Map<String, Map<String, List<TermMatch>>> scanResults) {
        boolean hit = hasAnyMatch(scanResults.get(f.getLexiconName()));

        FeatureHitSummaryRow row = new FeatureHitSummaryRow();
        row.setMessageId(messageId);
        row.setRunDate(runDate);
        row.setProcessId(processId);
        row.setPipelineExecId(pipelineExecId);
        row.setSentDate(sentDate);
        row.setMessageType(messageType);
        row.setFeatureId(f.getFeatureId());
        row.setFeatureName(f.getFeatureName());
        row.setFeatureType(f.getFeatureType());
        row.setFeatureIsNoiseReduction(f.getIsNoiseReductionRaw());
        if (f.isFromComposite()) {
            row.setSubFeatureType("lexicon");
            row.setSubFeatureName(f.getLexiconName());
        }
        row.setHitStatus(hit ? FeatureHitSummaryRow.HIT_YES : FeatureHitSummaryRow.HIT_NO);
        row.setCreatedTs(Timestamp.from(Instant.now()));
        return row;
    }

    // ── evaluated_lexicons construction ─────────────────────────────────────────

    /**
     * Builds one {@link EvaluatedLexicon} per (parent feature id, lexicon name)
     * combination that had at least one term hit. Entries with zero hits are
     * omitted entirely — {@code evaluated_lexicons} in {@code lexicon-hit-summary}
     * reports HITS, not exhaustive coverage (that is {@code feature-hit-summary}'s job).
     */
    private List<EvaluatedLexicon> buildEvaluatedLexicons(List<FeatureDecisionRow> allFeatures,
                                                            Map<String, Map<String, List<TermMatch>>> scanResults) {
        List<EvaluatedLexicon> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (FeatureDecisionRow f : allFeatures) {
            String key = f.getFeatureId() + "::" + f.getLexiconName();
            if (!seen.add(key)) continue;

            Map<String, List<TermMatch>> bySegment = scanResults.get(f.getLexiconName());
            if (!hasAnyMatch(bySegment)) continue; // no hit -> omit from evaluated_lexicons

            long totalTermsCount = resolveTotalTermsCount(f.getLexiconName());
            List<TermHitDetail> termDtls = buildTermDtls(bySegment);

            result.add(EvaluatedLexicon.of(f.getFeatureId(), f.getLexiconName(), totalTermsCount, termDtls));
        }
        return result;
    }

    /**
     * Groups matches by termId ACROSS all segments and builds one
     * {@link TermHitDetail} per distinct term, with its {@code matched_text}
     * JSON correctly attributing each match to the segment ("msg" or
     * "attachment-N") it was found in.
     */
    private List<TermHitDetail> buildTermDtls(Map<String, List<TermMatch>> bySegment) {
        // termId -> segment -> matches (preserves segment attribution per term)
        Map<String, Map<String, List<TermMatch>>> byTermThenSegment = new LinkedHashMap<>();

        for (Map.Entry<String, List<TermMatch>> segEntry : bySegment.entrySet()) {
            String segment = segEntry.getKey();
            for (TermMatch m : segEntry.getValue()) {
                byTermThenSegment
                        .computeIfAbsent(m.getTermId(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(segment, k -> new ArrayList<>())
                        .add(m);
            }
        }

        List<TermHitDetail> result = new ArrayList<>(byTermThenSegment.size());
        for (Map.Entry<String, Map<String, List<TermMatch>>> entry : byTermThenSegment.entrySet()) {
            String termId = entry.getKey();
            Map<String, List<TermMatch>> segments = entry.getValue();
            String pattern = segments.values().iterator().next().get(0).getTermRegexPattern();
            String matchedTextJson = buildMatchedTextJsonWithSegments(termId, segments);
            result.add(TermHitDetail.of(termId, pattern, matchedTextJson));
        }
        return result;
    }

    /**
     * Builds the {@code matched_text} JSON for one term, correctly scoped per
     * segment:
     * <pre>
     * {
     *   "msg": { "matches": [ {...} ] },
     *   "attachment-0": { "matches": [ {...} ] }
     * }
     * </pre>
     */
    private String buildMatchedTextJsonWithSegments(String termId, Map<String, List<TermMatch>> bySegment) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            for (Map.Entry<String, List<TermMatch>> seg : bySegment.entrySet()) {
                ObjectNode segNode = objectMapper.createObjectNode();
                ArrayNode matchArray = objectMapper.createArrayNode();
                for (TermMatch m : seg.getValue()) {
                    ObjectNode matchNode = objectMapper.createObjectNode();
                    matchNode.put("term_id", m.getTermId());
                    matchNode.put("match_text", m.getMatchText() != null ? m.getMatchText() : "");
                    matchNode.put("start_index", m.getStartIndex());
                    matchNode.put("end_index", m.getEndIndex());
                    matchNode.put("delta", m.getDelta());
                    matchArray.add(matchNode);
                }
                segNode.set("matches", matchArray);
                root.set(seg.getKey(), segNode);
            }
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            log.error("Failed to serialize matched_text for term '{}': {}", termId, e.getMessage(), e);
            return "{}";
        }
    }

    private long resolveTotalTermsCount(String lexiconName) {
        Map<Integer, TermManifestEntry> manifest = broadcastManifests.value().get(lexiconName);
        return manifest != null ? manifest.size() : 0L;
    }

    // ── lexicon-hit-restricted construction ─────────────────────────────────────

    private LexiconHitRestrictedRow buildRestrictedRow(String messageId, String processId,
                                                         List<EvaluatedLexicon> evaluatedLexicons) {
        LexiconHitRestrictedRow row = new LexiconHitRestrictedRow();
        row.setMessageId(messageId);
        row.setProcessId(processId);
        row.setPipelineExecId(pipelineExecId);
        row.setCreatedTs(Timestamp.from(Instant.now()));

        List<LexiconHitRestrictedRow.RestrictedEvaluatedLexicon> restricted = new ArrayList<>();
        for (EvaluatedLexicon e : evaluatedLexicons) {
            if (!e.hasHit()) continue;
            List<TermHitDetail> slimTermDtls = e.getTermDtls().stream()
                    .map(d -> TermHitDetail.ofRestricted(d.getTermId(), d.getMatchedText()))
                    .collect(Collectors.toList());
            restricted.add(LexiconHitRestrictedRow.RestrictedEvaluatedLexicon.of(e.getId(), slimTermDtls));
        }
        row.setEvaluatedLexicons(restricted);
        return row;
    }

    // ── Row field extraction helpers ──────────────────────────────────────────

    private static String getStringField(Row row, String fieldName) {
        try {
            int idx = row.fieldIndex(fieldName);
            return row.isNullAt(idx) ? null : row.getString(idx);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractAttachmentTexts(Row row) {
        try {
            int idx = row.fieldIndex("attachment_texts");
            if (row.isNullAt(idx)) return Collections.emptyList();
            scala.collection.Seq<Object> seq = row.getSeq(idx);
            List<String> result = new ArrayList<>();
            if (seq != null) {
                scala.collection.Iterator<Object> it = seq.iterator();
                while (it.hasNext()) {
                    Object val = it.next();
                    if (val != null) result.add(val.toString());
                }
            }
            return result;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * Extracts the features collected by {@code collect_list(struct(...))} in
     * the Spark aggregation step. Each element is a Spark Row representing one
     * {@link FeatureDecisionRow}.
     */
    @SuppressWarnings("unchecked")
    private static List<FeatureDecisionRow> extractFeatures(Row row) {
        try {
            int idx = row.fieldIndex("features");
            if (row.isNullAt(idx)) return Collections.emptyList();
            scala.collection.Seq<Row> seq = row.getSeq(idx);
            List<FeatureDecisionRow> result = new ArrayList<>();
            if (seq != null) {
                scala.collection.Iterator<Row> it = seq.iterator();
                while (it.hasNext()) {
                    result.add(rowToFeature(it.next()));
                }
            }
            return result;
        } catch (Exception e) {
            log.warn("Failed to extract features from row: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static FeatureDecisionRow rowToFeature(Row r) {
        return FeatureDecisionRow.of(
                getField(r, "message_id"), getField(r, "run_date"), getField(r, "process_id"),
                getField(r, "pipeline_exec_id"), getField(r, "sent_date"), getField(r, "message_type"),
                getField(r, "feature_id"), getField(r, "feature_type"), getField(r, "feature_name"),
                getField(r, "feature_operator"), getField(r, "is_noise_reduction_raw"),
                getField(r, "lexicon_name"),
                !r.isNullAt(r.fieldIndex("from_composite")) && r.getBoolean(r.fieldIndex("from_composite")),
                getField(r, "fm_feature_definition")
        );
    }

    private static String getField(Row row, String name) {
        try {
            int idx = row.fieldIndex(name);
            return row.isNullAt(idx) ? null : row.getString(idx);
        } catch (Exception e) {
            return null;
        }
    }
}
