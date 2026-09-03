package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanScanService;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermIdBuilder;
import com.db.macs3.ecomms.spectre.scanengine.html.HtmlStrippingService;
import com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.scanengine.model.match.RawExpressionMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageAttachment;
import com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata.TermEntry;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Scans one {@link FeatureDecisionRow} (one lexicon feature applied to one
 * message) against whatever areas its {@code feature_definition.body.scope}
 * covers, using a per-partition {@link HyperscanBundleLoader} for the
 * {@code .hdb} database and the accompanying per-term AND NOT/decomposition
 * metadata — both extracted from the same GCS zip bundle, see that class's
 * Javadoc. This is the real, Hyperscan/GCS-backed
 * {@link DecisionTreeEvaluator.FeatureRowScanner} implementation.
 *
 * <h2>Designed for reuse inside one {@code mapPartitions} call</h2>
 * <p>One instance is constructed per Spark partition (sharing that
 * partition's single {@link HyperscanBundleLoader}, and therefore its
 * cache), and its {@link #processMessage} is called once per message within
 * that partition. Nothing here performs a Spark action — every call operates
 * on exactly one message and returns plain, serialisable result objects,
 * safe to run entirely on an executor.
 *
 * <h2>Term id resolution and AND NOT evaluation</h2>
 * <p>A matched Hyperscan expression id is not always the term's own number
 * — see {@link TermExpressionMetadata} class Javadoc for the full id
 * scheme. {@code term_id} is always resolved via {@link TermIdBuilder},
 * never built directly from a raw matched expression id.
 *
 * <h2>Scan every area first, then resolve/evaluate once, across all of them</h2>
 * <p>{@link HyperscanScanService#scan} returns raw, un-resolved
 * {@link RawExpressionMatch}es per area. This class scans every area the
 * feature's scope covers, merges the raw results by expression id ACROSS
 * all of them (required and excluded patterns of the same AND NOT term can
 * legitimately match in different areas of the same message — a per-area
 * evaluation would miss this), then for every DISTINCT term any matched
 * expression id belongs to (via {@link TermExpressionMetadata}), evaluates:
 * the required side is satisfied iff EVERY entry of
 * {@code requiredExpressionIds} appears in the combined match-id set; for
 * an AND NOT term, the excluded side is satisfied (term therefore excluded)
 * iff EVERY entry of {@code excludedExpressionIds} also appears — same AND
 * convention on both sides. A term only produces a {@link TermMatchResult}
 * when the required side is satisfied AND the excluded side is not.
 *
 * <p>{@code term_regex_pattern} prefers {@link TermExpressionMetadata}'s own
 * pattern text over the raw match's expression text, since for a
 * COMBINATION match the expression text is the unreadable boolean formula
 * itself (e.g. {@code "(2&3&4)"}), not the term's actual pattern.
 *
 * <h2>Performance: one {@link Scanner}, and one HTML-strip pass, per message — never per feature</h2>
 * <p>A message can legitimately be evaluated against dozens of applicable
 * lexicon features (one {@link #scanRow} call each, all via the SAME
 * {@link #scannerFor} closure). Two anti-patterns this class deliberately
 * avoids, both scaling with (features × messages), not just messages:
 * <ul>
 *   <li><b>Constructing a new {@code Scanner} per scan call.</b> The wrapper
 *       library's own {@code Scanner} Javadoc: "you need one scanner
 *       instance per CPU thread" (reused across every scan that thread ever
 *       does) — it holds a native function-pointer callback and scratch
 *       space, both real, capped, per-JVM-process native resources (a
 *       hard limit of 256 live instances). This class owns exactly ONE
 *       {@link Scanner} for its entire lifetime (one instance = one Spark
 *       partition, matching Spark's own single-thread-per-task model) —
 *       see {@link #close}.</li>
 *   <li><b>Re-running {@code HtmlStrippingService.strip} per feature.</b>
 *       Subject/body/attachment text doesn't change across which feature is
 *       being scanned — only the {@code Database} does. {@link #scannerFor}
 *       strips every area's text EXACTLY ONCE per message
 *       ({@link #precomputeAreaTexts}), and every {@link #scanRow} call for
 *       that message reuses the same precomputed
 *       {@link HtmlStrippingService.StripResult}s. Attachment
 *       {@code cleanText} uses {@link HtmlStrippingService#identity} rather
 *       than {@link HtmlStrippingService#strip} — see
 *       {@code MessageAttachment} class Javadoc: it is already HTML-free by
 *       the time it reaches this engine, so the real stripping algorithm
 *       (and its {@code int[]} offset-map allocation) would be pure waste
 *       on text that can legitimately be megabytes long.</li>
 * </ul>
 */
public final class FeatureScanOrchestrator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FeatureScanOrchestrator.class);

    private final HyperscanBundleLoader bundleLoader;
    private final Long maxAttachmentSizeBytes;
    private final Scanner scanner = new Scanner();

    /**
     * @param bundleLoader              this partition's shared, cached zip-bundle loader — see
     *                                   {@link HyperscanBundleLoader} class Javadoc
     * @param maxAttachmentSizeBytes    null means unlimited — an attachment whose
     *                                   {@code cleanText} UTF-8 byte length exceeds this is
     *                                   skipped entirely (not scanned, not an error)
     */
    public FeatureScanOrchestrator(HyperscanBundleLoader bundleLoader, Long maxAttachmentSizeBytes) {
        this.bundleLoader = bundleLoader;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
    }

    /**
     * A {@link DecisionTreeEvaluator.FeatureRowScanner} bound to one message —
     * strips every area's text exactly once here (see class Javadoc), then
     * reuses that across however many {@link #scanRow} calls the caller
     * makes for this same message (one per applicable feature).
     */
    public DecisionTreeEvaluator.FeatureRowScanner scannerFor(ScanMessage message) {
        List<MessageAreaText> areaTexts = precomputeAreaTexts(message);
        return row -> scanRow(row, areaTexts);
    }

    /** One area's text, stripped exactly once per message — see class Javadoc. */
    private record MessageAreaText(MatchArea area, String attachmentId, String originalText,
                                    HtmlStrippingService.StripResult stripResult) {
    }

    private List<MessageAreaText> precomputeAreaTexts(ScanMessage message) {
        List<MessageAreaText> areaTexts = new ArrayList<>(2 + message.attachmentsOrEmpty().size());

        String subject = message.content() == null ? null : message.content().subject();
        if (subject != null && !subject.isBlank()) {
            areaTexts.add(new MessageAreaText(MatchArea.SUBJECT, null, subject, HtmlStrippingService.strip(subject)));
        }

        String rawText = message.content() == null ? null : message.content().rawText();
        if (rawText != null && !rawText.isBlank()) {
            areaTexts.add(new MessageAreaText(MatchArea.MESSAGE_BODY, null, rawText, HtmlStrippingService.strip(rawText)));
        }

        for (MessageAttachment attachment : message.attachmentsOrEmpty()) {
            MessageAreaText attachmentAreaText = toEligibleAttachmentAreaText(attachment);
            if (attachmentAreaText != null) {
                areaTexts.add(attachmentAreaText);
            }
        }
        return areaTexts;
    }

    /** @return the attachment's area text, or null if it's oversized or has no scannable content. */
    private MessageAreaText toEligibleAttachmentAreaText(MessageAttachment attachment) {
        if (!withinSizeLimit(attachment)) {
            return null;
        }
        String cleanText = attachment.cleanText();
        if (cleanText == null || cleanText.isBlank()) {
            return null;
        }
        // identity(), not strip(): attachment cleanText is already HTML-free — see class Javadoc.
        return new MessageAreaText(MatchArea.ATTACHMENT, attachment.attachmentId(), cleanText,
                HtmlStrippingService.identity(cleanText));
    }

    /** @return the scope constant (see {@code BqColumns.FeatureDefinitionJson}) a given area is gated by. */
    private static String scopeFor(MatchArea area) {
        return switch (area) {
            case SUBJECT -> BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT;
            case MESSAGE_BODY -> BqColumns.FeatureDefinitionJson.SCOPE_MESSAGE_BODY;
            case ATTACHMENT -> BqColumns.FeatureDefinitionJson.SCOPE_ATTACHMENT;
        };
    }

    /**
     * Scans one row's lexicon feature against every precomputed area its
     * scope covers, merges raw matches for the SAME expression id found in
     * more than one area, then resolves and evaluates every distinct term
     * referenced — see class Javadoc.
     */
    private List<TermMatchResult> scanRow(FeatureDecisionRow row, List<MessageAreaText> areaTexts) {
        FeatureDefinition definition = FeatureDefinition.parse(row.featureDefinitionJson());
        String feature = definition.body().feature();
        HyperscanBundleLoader.LexiconBundle bundle = bundleLoader.load(feature);
        Database database = bundle.database();
        TermExpressionMetadata metadata = bundle.metadata();

        List<RawExpressionMatch> allRawMatches = new ArrayList<>();
        List<AreaScanContext> areaScans = new ArrayList<>();

        for (MessageAreaText areaText : areaTexts) {
            if (!definition.body().hasScope(scopeFor(areaText.area()))) {
                continue;
            }
            List<RawExpressionMatch> matches = HyperscanScanService.scan(
                    areaText.stripResult(), database, areaText.area(), areaText.attachmentId(), scanner);
            allRawMatches.addAll(matches);
            areaScans.add(new AreaScanContext(areaText.area(), areaText.attachmentId(), areaText.originalText(), matches));
        }

        return resolveAndEvaluate(feature, metadata, allRawMatches, areaScans);
    }

    /**
     * One scanned area's raw matches AND its real original text — captured
     * here, at the scan call site, because it is not threaded through
     * {@link RawExpressionMatch}/{@link AreaMatch} today. Needed only by
     * terms with a {@code resolvedPatternTree} (see
     * {@link TermExpressionMetadata.TermEntry#requiresPerAreaEvaluation()}),
     * which must be verified against this SAME area's real text —
     * {@link ResolvedPatternAreaEvaluator} never receives text spanning more
     * than one area.
     */
    private record AreaScanContext(MatchArea area, String attachmentId, String originalText,
                                    List<RawExpressionMatch> rawMatches) {
    }

    /**
     * Releases this instance's single {@link Scanner} — call once, when this
     * partition's processing is entirely done (see {@code PartitionProcessor}'s
     * try-with-resources block). Never close and keep using the same
     * instance for more messages afterward.
     */
    @Override
    public void close() {
        scanner.close();
    }

    private boolean withinSizeLimit(MessageAttachment attachment) {
        if (maxAttachmentSizeBytes == null) {
            return true;
        }
        if (attachment.cleanText() == null) {
            return true;
        }
        long byteLength = attachment.cleanText().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return byteLength <= maxAttachmentSizeBytes;
    }

    /**
     * Merges raw matches ACROSS every scanned area by expression id, then
     * resolves and evaluates every distinct term referenced — see class
     * Javadoc "The fix: scan every area first, THEN resolve/evaluate once".
     *
     * <p>Terms with a {@code resolvedPatternTree} (see
     * {@link TermEntry#requiresPerAreaEvaluation()}) are evaluated by an
     * entirely separate branch below, PER SCANNED AREA, never merged into
     * this cross-area id-presence logic — word-distance/order is only
     * meaningful within one contiguous text. Do not "simplify" by folding
     * that branch into this merge; a required word in the subject and an
     * excluded word in the body legitimately share this cross-area boolean
     * check for the OLD id-list AND NOT scheme, but do NOT share a
     * coordinate space for a proximity/AND-NOT condition evaluated via
     * {@link ResolvedPatternAreaEvaluator} — conflating the two would
     * silently reintroduce a false-positive class of bug.
     */
    private List<TermMatchResult> resolveAndEvaluate(String feature, TermExpressionMetadata metadata,
                                                       List<RawExpressionMatch> allRawMatches,
                                                       List<AreaScanContext> areaScans) {
        Map<Integer, List<AreaMatch>> matchesByExpressionId = new LinkedHashMap<>();
        Map<Integer, String> patternTextByExpressionId = new LinkedHashMap<>();
        for (RawExpressionMatch rawMatch : allRawMatches) {
            matchesByExpressionId.computeIfAbsent(rawMatch.expressionId(), unusedKey -> new ArrayList<>())
                    .addAll(rawMatch.matches());
            patternTextByExpressionId.putIfAbsent(rawMatch.expressionId(), rawMatch.matchedPatternText());
        }
        Set<Integer> matchedExpressionIds = matchesByExpressionId.keySet();
        Map<Integer, TermEntry> termsToEvaluate = collectTermsToEvaluate(feature, metadata, matchedExpressionIds);

        List<TermMatchResult> results = new ArrayList<>();
        for (TermEntry entry : termsToEvaluate.values()) {
            TermMatchResult termMatchResult = buildTermMatchResult(
                    feature, entry, matchedExpressionIds, matchesByExpressionId, patternTextByExpressionId, areaScans);
            if (termMatchResult != null) {
                results.add(termMatchResult);
            }
        }
        return results;
    }

    /**
     * Every DISTINCT term any matched expression id belongs to, in first-seen order for
     * determinism — a term may be referenced by several matched ids (an AND NOT term's
     * required AND excluded sides both matching), so dedupe by term number. Also includes
     * every mandatory-per-area term (a resolvedPatternTree term with NO expression id at
     * all — a mandatory-per-area AND NOT term the Compile Service gave no id list for),
     * which is otherwise never discoverable via a matched expression id.
     */
    private Map<Integer, TermEntry> collectTermsToEvaluate(String feature, TermExpressionMetadata metadata,
                                                             Set<Integer> matchedExpressionIds) {
        Map<Integer, TermEntry> termsToEvaluate = new LinkedHashMap<>();
        Set<Integer> unrecognisedIds = new LinkedHashSet<>();
        for (int expressionId : matchedExpressionIds) {
            TermEntry entry = metadata.termByAnyExpressionId(expressionId);
            if (entry != null) {
                termsToEvaluate.putIfAbsent(entry.termNumber(), entry);
            } else {
                unrecognisedIds.add(expressionId);
            }
        }
        if (!unrecognisedIds.isEmpty()) {
            log.warn("feature='{}': {} matched expression id(s) not found in term metadata — skipping: {}",
                    feature, unrecognisedIds.size(), unrecognisedIds);
        }
        for (TermEntry entry : metadata.mandatoryPerAreaTerms()) {
            termsToEvaluate.putIfAbsent(entry.termNumber(), entry);
        }
        return termsToEvaluate;
    }

    /** @return this term's resolved match result, or null if the term did not produce any matches. */
    private TermMatchResult buildTermMatchResult(String feature, TermEntry entry, Set<Integer> matchedExpressionIds,
                                                  Map<Integer, List<AreaMatch>> matchesByExpressionId,
                                                  Map<Integer, String> patternTextByExpressionId,
                                                  List<AreaScanContext> areaScans) {
        List<AreaMatch> combined = entry.requiresPerAreaEvaluation()
                ? resolveAndEvaluatePerArea(feature, entry, matchedExpressionIds, areaScans)
                : resolveAndEvaluateCrossArea(feature, entry, matchedExpressionIds, matchesByExpressionId);
        if (combined.isEmpty()) {
            return null;
        }

        String termId = TermIdBuilder.build(feature, entry.termNumber());
        // Prefer the metadata's own pattern text (the resolvedPatterns string, or the
        // Compile Service's translatedPattern — always a genuine, readable pattern) over the
        // raw match's Expression text, which for a COMBINATION match is the unreadable
        // boolean formula string itself, and for an AND NOT match is only ONE leaf's own
        // text, not representative of the whole term.
        String termRegexPattern = entry.termRegexPattern() != null
                ? entry.termRegexPattern()
                : entry.hasCoarseExpressionId()
                        ? patternTextByExpressionId.get(entry.requiredExpressionIds().get(0))
                        : null;

        return new TermMatchResult(termId, termRegexPattern, combined);
    }

    /** Today's cross-area, id-presence-only evaluation — unchanged for any term without a resolvedPatternTree. */
    private List<AreaMatch> resolveAndEvaluateCrossArea(String feature, TermEntry entry,
                                                          Set<Integer> matchedExpressionIds,
                                                          Map<Integer, List<AreaMatch>> matchesByExpressionId) {
        boolean requiredSatisfied = matchedExpressionIds.containsAll(entry.requiredExpressionIds());
        boolean excludedSatisfied = entry.requiresExclusionCheck()
                && entry.excludedExpressionIds() != null
                && !entry.excludedExpressionIds().isEmpty()
                && matchedExpressionIds.containsAll(entry.excludedExpressionIds());

        if (!requiredSatisfied || excludedSatisfied) {
            if (entry.requiresExclusionCheck() && requiredSatisfied) {
                log.debug("feature='{}', term={}: excluded by AND NOT — required side matched but so did "
                        + "the excluded side", feature, entry.termNumber());
            }
            return List.of();
        }

        // Combine matches from EVERY required-side expression id — the excluded side is
        // never itself surfaced as a "hit" to report, only a condition already applied above.
        List<AreaMatch> combined = new ArrayList<>();
        for (int reqId : entry.requiredExpressionIds()) {
            combined.addAll(matchesByExpressionId.getOrDefault(reqId, List.of()));
        }
        return combined;
    }

    /**
     * New per-area evaluation for a term with a {@code resolvedPatternTree} —
     * see class Javadoc on {@link #resolveAndEvaluate} for why this must
     * never merge areas together. The term's coarse
     * {@code hyperscanExpressionId} (when available) is used only as a cheap
     * pre-filter, both globally and per area — never as the actual condition.
     */
    private List<AreaMatch> resolveAndEvaluatePerArea(String feature, TermEntry entry,
                                                        Set<Integer> matchedExpressionIds,
                                                        List<AreaScanContext> areaScans) {
        if (entry.hasCoarseExpressionId() && !matchedExpressionIds.containsAll(entry.requiredExpressionIds())) {
            return List.of(); // global pre-filter: at least one required leaf never matched anywhere at all
        }

        List<AreaMatch> combined = new ArrayList<>();
        for (AreaScanContext areaScan : areaScans) {
            if (!canSatisfyCondition(entry, areaScan)) {
                continue;
            }
            List<MatchSpan> spans = ResolvedPatternAreaEvaluator.findMatchingSpans(
                    entry.resolvedPatternTree(), areaScan.originalText());
            for (MatchSpan span : spans) {
                combined.add(new AreaMatch(areaScan.area(), areaScan.attachmentId(), span));
            }
        }
        return combined;
    }

    /** @return false if this area's text is empty, or (pre-filter) is missing a required leaf entirely. */
    private static boolean canSatisfyCondition(TermEntry entry, AreaScanContext areaScan) {
        if (areaScan.originalText() == null || areaScan.originalText().isBlank()) {
            return false;
        }
        if (!entry.hasCoarseExpressionId()) {
            return true;
        }
        Set<Integer> areaExpressionIds = areaScan.rawMatches().stream()
                .map(RawExpressionMatch::expressionId)
                .collect(Collectors.toSet());
        return areaExpressionIds.containsAll(entry.requiredExpressionIds());
    }
}
