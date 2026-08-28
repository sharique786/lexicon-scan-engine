package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanDatabaseLoader;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanScanService;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermIdBuilder;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermMetadataLoader;
import com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.RawExpressionMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.message.MessageAttachment;
import com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.TermExpressionMetadata.TermEntry;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import com.gliwka.hyperscan.wrapper.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scans one {@link FeatureDecisionRow} (one lexicon feature applied to one
 * message) against whatever areas its {@code feature_definition.body.scope}
 * covers, using a per-partition {@link HyperscanDatabaseLoader} for the
 * {@code .hdb} database and a per-partition {@link TermMetadataLoader} for
 * the accompanying per-term AND NOT/decomposition metadata — this is the
 * {@link DecisionTreeEvaluator.FeatureRowScanner} implementation
 * {@code DecisionTreeEvaluator} needs, wired to the real Hyperscan/GCS
 * layer rather than a test double.
 *
 * <h2>Designed for reuse inside one {@code mapPartitions} call</h2>
 * <p>One instance is constructed per Spark partition (sharing that
 * partition's single {@link HyperscanDatabaseLoader} and
 * {@link TermMetadataLoader}, and therefore their caches), and its
 * {@link #processMessage} is called once per message within that partition.
 * Nothing here performs a Spark action — every call operates on exactly one
 * message and returns plain, serialisable result objects, safe to run
 * entirely on an executor.
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
 */
public final class FeatureScanOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(FeatureScanOrchestrator.class);

    private final HyperscanDatabaseLoader databaseLoader;
    private final TermMetadataLoader metadataLoader;
    private final Long maxAttachmentSizeBytes;

    /**
     * @param databaseLoader            this partition's shared, cached {@code .hdb} loader
     * @param metadataLoader            this partition's shared, cached term-metadata loader — see
     *                                   {@link TermMetadataLoader} class Javadoc
     * @param maxAttachmentSizeBytes    null means unlimited (requirement 4.e default) — an
     *                                   attachment whose {@code cleanText} UTF-8 byte length
     *                                   exceeds this is skipped entirely (not scanned, not an error)
     */
    public FeatureScanOrchestrator(HyperscanDatabaseLoader databaseLoader, TermMetadataLoader metadataLoader,
                                    Long maxAttachmentSizeBytes) {
        this.databaseLoader = databaseLoader;
        this.metadataLoader = metadataLoader;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
    }

    /** A {@link DecisionTreeEvaluator.FeatureRowScanner} bound to one message — see {@code processMessage}. */
    public DecisionTreeEvaluator.FeatureRowScanner scannerFor(ScanMessage message) {
        return row -> scanRow(row, message);
    }

    /**
     * Scans one row's lexicon feature against every area its scope covers,
     * merges raw matches for the SAME expression id found in more than one
     * area, then resolves and evaluates every distinct term referenced —
     * see class Javadoc.
     */
    private List<TermMatchResult> scanRow(FeatureDecisionRow row, ScanMessage message) {
        FeatureDefinition definition = FeatureDefinition.parse(row.featureDefinitionJson());
        String feature = definition.body().feature();
        Database database = databaseLoader.load(feature);
        TermExpressionMetadata metadata = metadataLoader.load(feature);

        List<RawExpressionMatch> allRawMatches = new ArrayList<>();

        if (definition.body().hasScope(BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT)) {
            allRawMatches.addAll(HyperscanScanService.scan(
                    message.content() == null ? null : message.content().subject(),
                    database, MatchArea.SUBJECT, null));
        }

        if (definition.body().hasScope(BqColumns.FeatureDefinitionJson.SCOPE_MESSAGE_BODY)) {
            allRawMatches.addAll(HyperscanScanService.scan(
                    message.content() == null ? null : message.content().rawText(),
                    database, MatchArea.MESSAGE_BODY, null));
        }

        if (definition.body().hasScope(BqColumns.FeatureDefinitionJson.SCOPE_ATTACHMENT)) {
            for (MessageAttachment attachment : message.attachmentsOrEmpty()) {
                if (!withinSizeLimit(attachment)) {
                    continue;
                }
                allRawMatches.addAll(HyperscanScanService.scan(
                        attachment.cleanText(), database, MatchArea.ATTACHMENT, attachment.attachmentId()));
            }
        }

        return resolveAndEvaluate(feature, metadata, allRawMatches);
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
     */
    private List<TermMatchResult> resolveAndEvaluate(String feature, TermExpressionMetadata metadata,
                                                       List<RawExpressionMatch> allRawMatches) {
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

        List<TermMatchResult> results = new ArrayList<>();
        for (TermEntry entry : termsToEvaluate.values()) {
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
                continue;
            }

            // Combine matches from EVERY required-side expression id — the excluded side is
            // never itself surfaced as a "hit" to report, only a condition already applied above.
            List<AreaMatch> combined = new ArrayList<>();
            for (int reqId : entry.requiredExpressionIds()) {
                combined.addAll(matchesByExpressionId.getOrDefault(reqId, List.of()));
            }

            String termId = TermIdBuilder.build(feature, entry.termNumber());
            // Prefer the metadata's own pattern text (the Compile Service's translatedPattern,
            // always a genuine, readable pattern) over the raw match's Expression text, which
            // for a COMBINATION match is the unreadable boolean formula string itself, and for
            // an AND NOT match is only ONE leaf's own text, not representative of the whole term.
            String termRegexPattern = entry.termRegexPattern() != null
                    ? entry.termRegexPattern()
                    : patternTextByExpressionId.get(entry.requiredExpressionIds().get(0));

            results.add(new TermMatchResult(termId, termRegexPattern, combined));
        }
        return results;
    }
}
