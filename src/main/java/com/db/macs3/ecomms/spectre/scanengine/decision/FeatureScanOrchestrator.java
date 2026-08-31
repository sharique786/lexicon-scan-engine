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
 * {@code .hdb} database AND the accompanying per-term AND NOT/decomposition
 * metadata — both now extracted from the same GCS zip bundle, see that
 * class's Javadoc — this is the {@link DecisionTreeEvaluator.FeatureRowScanner}
 * implementation {@code DecisionTreeEvaluator} needs, wired to the real
 * Hyperscan/GCS layer rather than a test double.
 *
 * <h2>Designed for reuse inside one {@code mapPartitions} call</h2>
 * <p>One instance is constructed per Spark partition (sharing that
 * partition's single {@link HyperscanBundleLoader}, and therefore its
 * cache), and its {@link #processMessage} is called once per message within
 * that partition. Nothing here performs a Spark action — every call operates
 * on exactly one message and returns plain, serialisable result objects,
 * safe to run entirely on an executor.
 *
 * <h2>Confirmed gap this class fixes: AND NOT / decomposed term_id and hit evaluation</h2>
 * <p>An earlier version of this class resolved a matched Hyperscan
 * expression id directly to a {@code term_id} via
 * {@code TermIdBuilder.build(feature, expressionId)}, on the documented
 * assumption that this id was always the term's own number. The Compile
 * Service's AND NOT fix broke that assumption for AND NOT terms
 * specifically — see {@link TermExpressionMetadata} class Javadoc for the
 * full explanation. Confirmed, concrete consequences of the old code,
 * traced through to production impact:
 * <ul>
 *   <li>{@code lexicon-hit-summary.evaluated_lexicons.term_dtls.term_id} was
 *       populated with a wrong, meaningless value for any AND NOT term's
 *       required/excluded pattern match (an arbitrary allocated id, not the
 *       term's number).</li>
 *   <li>{@code DecisionTreeEvaluator}'s hit check
 *       ({@code !matches.isEmpty()}) had no AND NOT boolean logic to rely
 *       on at all — a message containing ONLY the excluded pattern, with
 *       the required pattern entirely absent, would still register as a
 *       feature hit, a real false-positive risk, not merely a labelling issue.</li>
 *   <li>{@code term_regex_pattern} for a purely-decomposed (non-AND-NOT)
 *       term would show the unreadable native COMBINATION formula string
 *       (e.g. {@code "(2&3&4)"}) rather than the term's actual pattern
 *       text, since the match's own {@code Expression} text IS that
 *       formula for a combination match — a separate, pre-existing
 *       display bug this rewrite also fixes by preferring
 *       {@link TermExpressionMetadata}'s own {@code termRegexPattern}
 *       (built from the Compile Service's {@code translatedPattern} list)
 *       over the raw match's expression text.</li>
 * </ul>
 *
 * <h2>The fix: scan every area first, THEN resolve/evaluate once, across all of them</h2>
 * <p>{@link HyperscanScanService#scan} now returns raw,
 * un-resolved {@link RawExpressionMatch}es per area — see that class's
 * Javadoc for why resolution moved out of it. This class scans every area
 * the feature's scope covers, merges the raw results by expression id
 * ACROSS all of them (required and excluded patterns of the same AND NOT
 * term can legitimately match in different areas of the same message — a
 * per-area evaluation would miss this), then for every DISTINCT term any
 * matched expression id belongs to (via {@link TermExpressionMetadata}),
 * evaluates: the required side is satisfied iff EVERY entry of
 * {@code requiredExpressionIds} appears in the combined match-id set; for
 * an AND NOT term, the excluded side is satisfied (term therefore excluded)
 * iff EVERY entry of {@code excludedExpressionIds} also appears — same AND
 * convention on both sides, mirroring the Lexicon Compile Service's and
 * Lexicon Scanner Service's own documented AND NOT contracts exactly, for
 * consistency across all three services. A term only produces a
 * {@link TermMatchResult} when the required side is satisfied AND the
 * excluded side is not.
 *
 * <h2>Performance: one {@link Scanner}, and one HTML-strip pass, per message — never per feature</h2>
 * <p>A message can legitimately be evaluated against dozens of applicable
 * lexicon features (one {@link #scanRow} call each, all via the SAME
 * {@link #scannerFor} closure). Two real, measured anti-patterns this class
 * deliberately avoids, both scaling with (features × messages), not just
 * messages — significant at "millions of messages" volume:
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
 *       Subject/body/attachment TEXT doesn't change across which feature is
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
     * @param maxAttachmentSizeBytes    null means unlimited (requirement 4.e default) — an
     *                                   attachment whose {@code cleanText} UTF-8 byte length
     *                                   exceeds this is skipped entirely (not scanned, not an error)
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
            if (!withinSizeLimit(attachment)) {
                continue;
            }
            String cleanText = attachment.cleanText();
            if (cleanText == null || cleanText.isBlank()) {
                continue;
            }
            // identity(), not strip(): attachment cleanText is already HTML-free — see class Javadoc.
            areaTexts.add(new MessageAreaText(MatchArea.ATTACHMENT, attachment.attachmentId(), cleanText,
                    HtmlStrippingService.identity(cleanText)));
        }
        return areaTexts;
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
        for (RawExpressionMatch rem : allRawMatches) {
            matchesByExpressionId.computeIfAbsent(rem.expressionId(), k -> new ArrayList<>()).addAll(rem.matches());
            patternTextByExpressionId.putIfAbsent(rem.expressionId(), rem.matchedPatternText());
        }
        Set<Integer> matchedExpressionIds = matchesByExpressionId.keySet();

        // Every DISTINCT term any matched expression id belongs to, in first-seen order for
        // determinism — a term may be referenced by several matched ids (an AND NOT term's
        // required AND excluded sides both matching), so dedupe by term number.
        Map<Integer, TermEntry> termsToEvaluate = new LinkedHashMap<>();
        Set<Integer> unrecognisedIds = new LinkedHashSet<>();
        for (int id : matchedExpressionIds) {
            TermEntry entry = metadata.termByAnyExpressionId(id);
            if (entry == null) {
                unrecognisedIds.add(id);
                continue;
            }
            termsToEvaluate.putIfAbsent(entry.termNumber(), entry);
        }
        if (!unrecognisedIds.isEmpty()) {
            log.warn("feature='{}': {} matched expression id(s) not found in term metadata — skipping: {}",
                    feature, unrecognisedIds.size(), unrecognisedIds);
        }
        // A resolvedPatternTree term with NO expression id at all (a mandatory-per-area AND NOT
        // term the Compile Service gave no id list for) is never discoverable via a matched
        // expression id above — this is the only way it's ever found.
        for (TermEntry entry : metadata.mandatoryPerAreaTerms()) {
            termsToEvaluate.putIfAbsent(entry.termNumber(), entry);
        }

        List<TermMatchResult> results = new ArrayList<>();
        for (TermEntry entry : termsToEvaluate.values()) {
            List<AreaMatch> combined = entry.requiresPerAreaEvaluation()
                    ? resolveAndEvaluatePerArea(feature, entry, matchedExpressionIds, areaScans)
                    : resolveAndEvaluateCrossArea(feature, entry, matchedExpressionIds, matchesByExpressionId);
            if (combined.isEmpty()) {
                continue;
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

            results.add(new TermMatchResult(termId, termRegexPattern, combined));
        }
        return results;
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
        for (AreaScanContext ctx : areaScans) {
            if (ctx.originalText() == null || ctx.originalText().isBlank()) {
                continue;
            }
            if (entry.hasCoarseExpressionId()) {
                Set<Integer> areaIds = ctx.rawMatches().stream()
                        .map(RawExpressionMatch::expressionId)
                        .collect(Collectors.toSet());
                if (!areaIds.containsAll(entry.requiredExpressionIds())) {
                    continue; // per-area pre-filter: this area can't possibly satisfy the condition
                }
            }
            List<MatchSpan> spans = ResolvedPatternAreaEvaluator.findMatchingSpans(
                    entry.resolvedPatternTree(), ctx.originalText());
            for (MatchSpan span : spans) {
                combined.add(new AreaMatch(ctx.area(), ctx.attachmentId(), span));
            }
        }
        return combined;
    }
}
