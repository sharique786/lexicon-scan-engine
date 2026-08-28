package com.db.macs3.ecomms.spectre.scanengine.model.termmeta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses and indexes one feature's per-term expression-id metadata — the
 * Lexicon Compile Service's {@code /compile/bundle} JSON response (the same
 * {@code CompileResponse}/{@code TermCompilationResult} shape
 * {@code /compile} returns), read from the {@code <feature>-compile-results.json}
 * file the Compile Service writes alongside every {@code <feature>.hdb} —
 * see {@code TermMetadataLoader} class Javadoc for why this is now read at
 * all, and {@code HyperscanScanService} / {@code FeatureScanOrchestrator}
 * class Javadocs for how the id scheme this indexes is actually used.
 *
 * <h2>Why this is needed now — confirmed gap</h2>
 * <p>Before the Compile Service's AND NOT fix (see
 * {@code HyperscanCombinationHandler} class Javadoc, Compile Service
 * project), every PASS term's reportable Hyperscan expression id — AND NOT
 * or not — was always that term's own term number, so a matched expression
 * id could be turned directly into a {@code term_id} string via
 * {@code TermIdBuilder.build(feature, expressionId)} with no other
 * information needed. Confirmed unreliable and fixed: Hyperscan's own
 * documented eager, progressive combination evaluation means a formula
 * mixing a positive requirement with a negation (the old AND NOT combination
 * shape) can fire before the negated pattern has been reached by the scan at
 * all. The fix removed native COMBINATION for AND NOT terms entirely —
 * every required/excluded pattern of an AND NOT term now compiles as its
 * own plain, individually-reportable expression, using an ALLOCATED id that
 * is NOT the term's own number. Resolving a matched expression id back to
 * the correct {@code term_id}, and correctly evaluating the AND NOT boolean
 * condition at all, now requires this metadata — the {@code .hdb} file alone
 * is no longer self-sufficient for AND NOT terms (it remains self-sufficient
 * for simple and purely-decomposed terms, which are unaffected by this fix).
 *
 * <h2>Second schema change: {@code resolvedPatterns} / decomposed NEAR, FOLLOWEDBY, AND NOT</h2>
 * <p>The Compile Service later changed its schema again, specifically for
 * terms using {@code NEAR{n}}/{@code FOLLOWEDBY{n}} proximity operators (and,
 * potentially, AND NOT terms too): a complex term can now be split
 * ("Pattern Too Large") into multiple decomposed {@code regexPattern} leaves
 * (renamed from {@code translatedPattern}), each compiled with the
 * {@code QUIET} Hyperscan flag — confirmed against a real compiled
 * {@code .hdb} dump. QUIET means Hyperscan's own match callback NEVER
 * reports an individual leaf's matches; only the wrapping native
 * {@code COMBINATION} expression (the term's {@code hyperscanExpressionId})
 * fires, and firing only proves "every leaf matched somewhere in this one
 * scan buffer" — no order/distance information for the leaves is ever
 * recoverable from Hyperscan itself for these terms. The new
 * {@code resolvedPatterns} field (present iff this decomposition applies)
 * carries the leaves' operator structure as text (e.g.
 * {@code "manipulate NEAR{5} (?:price|spread|stock)"}); see
 * {@link ResolvedPatternTree#build} for how that text's SHAPE is parsed and
 * zipped against the structured {@code regexPattern} leaf list, and
 * {@code ResolvedPatternAreaEvaluator}/{@code FeatureScanOrchestrator} for
 * how the resulting tree is evaluated per scanned area against the message's
 * real original text — the only way to genuinely verify the proximity/
 * AND-NOT condition, since Hyperscan cannot.
 *
 * <p>A term's {@code resolvedPatterns} field (non-blank) is the sole,
 * per-term discriminator between this new evaluation path and the existing
 * cross-area, id-presence-only evaluation path above — a term without it
 * (whatever its {@code requiresExclusionCheck} value) is handled completely
 * unchanged from before this schema change.
 *
 * <h2>One TermEntry per term, indexed two ways</h2>
 * <p>{@link #termByAnyExpressionId(int)} maps ANY expression id this
 * feature's {@code .hdb} might report — whether a non-AND-NOT term's own
 * reportable id, or one of an AND NOT term's required/excluded ids — back to
 * the {@link TermEntry} it belongs to. This is the lookup
 * {@code FeatureScanOrchestrator} uses for terms with a matchable expression
 * id. A {@link TermEntry} with {@link TermEntry#requiresPerAreaEvaluation()}
 * true but no expression id at all (a mandatory-per-area AND NOT term the
 * Compile Service gave no id list for — see class Javadoc above) is NOT
 * reachable this way at all; {@link #mandatoryPerAreaTerms()} is the only
 * way to discover it.
 *
 * <p>Immutable and safe to cache/share across every message a Spark
 * partition processes for one feature (mirrors
 * {@code HyperscanDatabaseLoader}'s per-partition {@code Database} caching —
 * see {@code TermMetadataLoader}).
 */
public final class TermExpressionMetadata implements Serializable {

    private final String feature;
    private final Map<Integer, TermEntry> byExpressionId;
    private final Map<Integer, TermEntry> byTermNumber;

    private TermExpressionMetadata(String feature, Map<Integer, TermEntry> byExpressionId,
                                    Map<Integer, TermEntry> byTermNumber) {
        this.feature = feature;
        this.byExpressionId = byExpressionId;
        this.byTermNumber = byTermNumber;
    }

    /** @return the term entry owning {@code expressionId}, or null if unrecognised. */
    public TermEntry termByAnyExpressionId(int expressionId) {
        return byExpressionId.get(expressionId);
    }

    /**
     * @return every term that requires per-area {@code resolvedPatterns} tree
     *         evaluation but has NO expression id at all to key a
     *         {@link #termByAnyExpressionId} lookup off of — the only way
     *         such a term is ever discovered. See class Javadoc.
     */
    public List<TermEntry> mandatoryPerAreaTerms() {
        List<TermEntry> result = new ArrayList<>();
        for (TermEntry entry : byTermNumber.values()) {
            if (entry.requiresPerAreaEvaluation() && !entry.hasCoarseExpressionId()) {
                result.add(entry);
            }
        }
        return result;
    }

    /** @return the feature these terms belong to (verbatim {@code body.feature}). */
    public String feature() {
        return feature;
    }

    /** @return how many distinct terms this feature's metadata describes. */
    public int termCount() {
        return byTermNumber.size();
    }

    /**
     * One term's expression-id shape, resolved from a
     * {@code TermCompilationResult} JSON entry.
     *
     * @param termNumber             parsed from {@code termId}'s {@code ::<n>} suffix —
     *                               what {@code TermIdBuilder.build(feature, termNumber)} needs
     *                               to build the CORRECT {@code term_id} string, regardless of
     *                               which raw expression id actually matched
     * @param termRegexPattern       the term's pattern text for display — the verbatim
     *                               {@code resolvedPatterns} string when present (preserves the
     *                               NEAR/FOLLOWEDBY operator and distance for analyst readability),
     *                               else the required side's pattern text joined for display. Used
     *                               only when the {@code .hdb}'s own embedded pattern text (read
     *                               directly off the {@link com.gliwka.hyperscan.wrapper.Match}) is
     *                               unavailable or unreliable — see {@code FeatureScanOrchestrator}
     * @param requiresExclusionCheck true for AND NOT terms
     * @param requiredExpressionIds  every expression id belonging to this term's required side, when
     *                               any exist. ALWAYS populated for a term without
     *                               {@code resolvedPatternTree} (legacy behavior, unchanged) — exactly
     *                               one entry for a simple/purely-decomposed term (that one entry IS
     *                               the term's own reportable id), one entry per required pattern for
     *                               a legacy AND NOT term. MAY be null for a term WITH a
     *                               {@code resolvedPatternTree} whose AND NOT shape the Compile Service
     *                               gave no id list for at all — see {@link #hasCoarseExpressionId()}
     *                               and {@link #mandatoryPerAreaTerms()}.
     * @param excludedExpressionIds  every expression id belonging to this term's excluded side.
     *                               Null/empty unless {@code requiresExclusionCheck}.
     * @param resolvedPatternTree    non-null iff this term's JSON entry carried a non-blank
     *                               {@code resolvedPatterns} — see class Javadoc "Second schema
     *                               change" section. When non-null, {@code FeatureScanOrchestrator}
     *                               must evaluate this term PER SCANNED AREA independently against
     *                               that area's real original text, never merged across areas —
     *                               word-distance across two different texts is meaningless.
     */
    public record TermEntry(
            int termNumber,
            String termRegexPattern,
            boolean requiresExclusionCheck,
            List<Integer> requiredExpressionIds,
            List<Integer> excludedExpressionIds,
            ResolvedPatternTree resolvedPatternTree
    ) {
        /**
         * @return true when this term uses the (still-safe) native Hyperscan
         *         COMBINATION mechanism — pure decomposition, no AND NOT — meaning
         *         its {@link #requiredExpressionIds} has exactly one entry that IS
         *         the term's own reportable id, no further boolean evaluation needed.
         *         Always false for a term with a {@link #resolvedPatternTree} — such
         *         a term always needs {@code ResolvedPatternAreaEvaluator} verification,
         *         even a Chain-shaped (non-AND-NOT) one, since its coarse COMBINATION id
         *         alone cannot confirm the actual proximity distance/order.
         */
        public boolean isNativelyResolved() {
            return !requiresExclusionCheck && resolvedPatternTree == null;
        }

        /** @return true iff this term needs {@code ResolvedPatternAreaEvaluator}-style per-area evaluation. */
        public boolean requiresPerAreaEvaluation() {
            return resolvedPatternTree != null;
        }

        /** @return true iff {@link #requiredExpressionIds} gives a usable coarse pre-filter id set. */
        public boolean hasCoarseExpressionId() {
            return requiredExpressionIds != null && !requiredExpressionIds.isEmpty();
        }
    }

    // ── Parsing ───────────────────────────────────────────────────────────

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses one feature's {@code <feature>-compile-results.json} content —
     * the Compile Service's {@code CompileResponse} shape — into an indexed
     * {@link TermExpressionMetadata}.
     *
     * @throws TermMetadataParseException on malformed JSON, a term missing a
     *         parseable {@code ::<n>} term number, an expression id
     *         appearing under more than one term, or a {@code resolvedPatterns}
     *         value structurally inconsistent with its {@code regexPattern}/
     *         {@code patternMapping}/{@code requiresExclusionCheck}/
     *         {@code hyperscanExpressionId} siblings (both indicate the JSON
     *         does not genuinely describe this feature's {@code .hdb})
     */
    public static TermExpressionMetadata parse(String feature, String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new TermMetadataParseException(
                    "term metadata JSON for feature '" + feature + "' is null or blank");
        }
        CompileResponseJson parsed;
        try {
            parsed = MAPPER.readValue(rawJson, CompileResponseJson.class);
        } catch (IOException e) {
            throw new TermMetadataParseException(
                    "Could not parse term metadata JSON for feature '" + feature + "': " + e.getMessage(), e);
        }

        Map<Integer, TermEntry> byExpressionId = new HashMap<>();
        Map<Integer, TermEntry> byTermNumber = new HashMap<>();
        if (parsed.results() != null) {
            for (TermResultJson r : parsed.results()) {
                if (!"PASS".equalsIgnoreCase(r.compilationStatus())) {
                    continue; // FAILED terms were never compiled into the .hdb — no ids to index
                }
                int termNumber = parseTermNumber(feature, r.termId());
                List<String> leaves = patternLeaves(r);
                boolean requiresExclusion = Boolean.TRUE.equals(r.requiresExclusionCheck());

                ResolvedPatternTree tree = null;
                if (r.resolvedPatterns() != null && !r.resolvedPatterns().isBlank()) {
                    tree = ResolvedPatternTree.build(feature, r.termId(), r.resolvedPatterns(), leaves);
                    validateShapeAgreement(feature, r, tree, requiresExclusion);
                }

                String termRegexPattern = tree != null
                        ? r.resolvedPatterns()
                        : (leaves == null ? null : String.join(" & ", leaves));

                List<Integer> requiredIds;
                List<Integer> excludedIds;
                if (tree instanceof ResolvedPatternTree.AndNot) {
                    // A resolvedPatterns AND NOT term's ids (if the Compile Service supplies any at
                    // all) come verbatim from requiredExpressionIds/excludedExpressionIds — no
                    // throw-if-absent check, unlike resolveRequiredIds below, since such a term may
                    // legitimately have NONE at all (see mandatoryPerAreaTerms()).
                    requiredIds = r.requiredExpressionIds();
                    excludedIds = r.excludedExpressionIds();
                } else if (tree instanceof ResolvedPatternTree.Chain) {
                    requiredIds = resolveRequiredIds(feature, r, termNumber);
                    excludedIds = null;
                } else {
                    // Legacy path (no resolvedPatterns) — completely unchanged.
                    requiredIds = resolveRequiredIds(feature, r, termNumber);
                    excludedIds = r.excludedExpressionIds();
                }

                TermEntry entry = new TermEntry(
                        termNumber, termRegexPattern, requiresExclusion, requiredIds, excludedIds, tree);

                putUnique(byTermNumber, feature, termNumber, entry);
                if (entry.requiredExpressionIds() != null) {
                    for (int id : entry.requiredExpressionIds()) {
                        putUniqueExpressionId(byExpressionId, feature, id, entry);
                    }
                }
                if (entry.excludedExpressionIds() != null) {
                    for (int id : entry.excludedExpressionIds()) {
                        putUniqueExpressionId(byExpressionId, feature, id, entry);
                    }
                }
            }
        }
        return new TermExpressionMetadata(feature, byExpressionId, byTermNumber);
    }

    /** {@code regexPattern} (new schema) if present, else {@code translatedPattern} (old schema). */
    private static List<String> patternLeaves(TermResultJson r) {
        return r.regexPattern() != null ? r.regexPattern() : r.translatedPattern();
    }

    /**
     * Cross-checks a {@code resolvedPatterns}-bearing term's shape against
     * its declared {@code requiresExclusionCheck}, {@code hyperscanExpressionId},
     * and {@code patternMapping} siblings — these are all supposed to agree on
     * whether the term is a plain proximity chain or an AND NOT condition, and
     * a disagreement means a real compiled example contradicts an assumption
     * this whole project (and the Compile Service's own documented AND NOT
     * fix) depends on, which must surface loudly rather than silently
     * mis-evaluate. See class Javadoc "Second schema change" section.
     */
    private static void validateShapeAgreement(String feature, TermResultJson r, ResolvedPatternTree tree,
                                                boolean requiresExclusion) {
        boolean isAndNotShape = tree instanceof ResolvedPatternTree.AndNot;
        if (requiresExclusion != isAndNotShape) {
            throw new TermMetadataParseException(
                    "Term '" + r.termId() + "' in feature '" + feature + "': requiresExclusionCheck="
                    + requiresExclusion + " does not agree with resolvedPatterns' shape ("
                    + (isAndNotShape ? "AND NOT" : "plain chain") + ") — malformed compile-results JSON.");
        }
        if (isAndNotShape) {
            if (r.hyperscanExpressionId() != null) {
                throw new TermMetadataParseException(
                        "Term '" + r.termId() + "' in feature '" + feature + "' is an AND NOT term (per "
                        + "resolvedPatterns) but also has a native hyperscanExpressionId="
                        + r.hyperscanExpressionId() + " populated. This contradicts the documented "
                        + "assumption that AND NOT terms never use native COMBINATION (see "
                        + "TermExpressionMetadata class Javadoc) — re-verify TermExpressionMetadata's "
                        + "AND NOT handling against this real example before trusting this feature's output.");
            }
            if (r.patternMapping() != null && !r.patternMapping().isBlank()) {
                throw new TermMetadataParseException(
                        "Term '" + r.termId() + "' in feature '" + feature + "' is an AND NOT term (per "
                        + "resolvedPatterns) but also has a patternMapping value (" + r.patternMapping()
                        + ") populated. This contradicts the documented assumption that AND NOT terms "
                        + "never use native COMBINATION — re-verify TermExpressionMetadata's AND NOT "
                        + "handling against this real example before trusting this feature's output.");
            }
        } else {
            validatePatternMappingCount(feature, r, (ResolvedPatternTree.Chain) tree);
        }
    }

    /**
     * When {@code patternMapping} is present on a plain (non-AND-NOT) chain
     * term, its id count must match the chain's own leaf count — e.g.
     * {@code "(7&8)"} for a 2-leaf chain. Optional field: a chain term with no
     * {@code patternMapping} at all (e.g. a single-leaf term with no
     * decomposition) is not checked.
     */
    private static void validatePatternMappingCount(String feature, TermResultJson r, ResolvedPatternTree.Chain chain) {
        String mapping = r.patternMapping();
        if (mapping == null || mapping.isBlank()) {
            return;
        }
        String stripped = mapping.trim();
        if (stripped.startsWith("(") && stripped.endsWith(")")) {
            stripped = stripped.substring(1, stripped.length() - 1);
        }
        int idCount = stripped.isBlank() ? 0 : stripped.split("&").length;
        if (idCount != chain.leaves().size()) {
            throw new TermMetadataParseException(
                    "Term '" + r.termId() + "' in feature '" + feature + "': patternMapping '" + mapping
                    + "' implies " + idCount + " expression id(s) but regexPattern/translatedPattern has "
                    + chain.leaves().size() + " leaf/leaves — malformed compile-results JSON.");
        }
    }

    /**
     * A non-AND-NOT term reports {@code hyperscanExpressionId} (singular);
     * an AND NOT term reports {@code requiredExpressionIds} directly. Either
     * way this returns the required-side id list {@link TermEntry} needs.
     */
    private static List<Integer> resolveRequiredIds(String feature, TermResultJson r, int termNumber) {
        if (r.requiredExpressionIds() != null && !r.requiredExpressionIds().isEmpty()) {
            return r.requiredExpressionIds();
        }
        if (r.hyperscanExpressionId() != null) {
            return List.of(r.hyperscanExpressionId());
        }
        throw new TermMetadataParseException(
                "Term '" + r.termId() + "' in feature '" + feature + "' is PASS but has neither "
                + "hyperscanExpressionId nor requiredExpressionIds populated — malformed compile-results JSON.");
    }

    private static void putUnique(Map<Integer, TermEntry> map, String feature, int termNumber, TermEntry entry) {
        TermEntry existing = map.putIfAbsent(termNumber, entry);
        if (existing != null) {
            throw new TermMetadataParseException(
                    "Term number " + termNumber + " in feature '" + feature + "' appears more than once — "
                    + "malformed or stale compile-results JSON.");
        }
    }

    private static void putUniqueExpressionId(Map<Integer, TermEntry> map, String feature, int id, TermEntry entry) {
        TermEntry existing = map.putIfAbsent(id, entry);
        if (existing != null && existing.termNumber() != entry.termNumber()) {
            throw new TermMetadataParseException(
                    "Expression id " + id + " in feature '" + feature + "' is claimed by both term "
                    + existing.termNumber() + " and term " + entry.termNumber()
                    + " — malformed or stale compile-results JSON.");
        }
    }

    private static int parseTermNumber(String feature, String termId) {
        if (termId == null) {
            throw new TermMetadataParseException("A term in feature '" + feature + "' has a null termId.");
        }
        int sep = termId.lastIndexOf("::");
        if (sep < 0) {
            throw new TermMetadataParseException(
                    "termId '" + termId + "' in feature '" + feature + "' does not end with '::<n>'.");
        }
        try {
            return Integer.parseInt(termId.substring(sep + 2));
        } catch (NumberFormatException e) {
            throw new TermMetadataParseException(
                    "termId '" + termId + "' in feature '" + feature + "' has a non-numeric suffix.", e);
        }
    }

    // ── Raw JSON shape (only the fields this class needs) ───────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CompileResponseJson(
            @JsonProperty("results") List<TermResultJson> results
    ) {
        @JsonCreator
        private CompileResponseJson {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record TermResultJson(
            @JsonProperty("termId") String termId,
            @JsonProperty("termDescription") String termDescription,
            @JsonProperty("compilationStatus") String compilationStatus,
            @JsonProperty("translatedPattern") List<String> translatedPattern,
            @JsonProperty("regexPattern") List<String> regexPattern,
            @JsonProperty("requiresExclusionCheck") Boolean requiresExclusionCheck,
            @JsonProperty("resolvedPatterns") String resolvedPatterns,
            @JsonProperty("hyperscanExpressionId") Integer hyperscanExpressionId,
            @JsonProperty("requiredExpressionIds") List<Integer> requiredExpressionIds,
            @JsonProperty("excludedExpressionIds") List<Integer> excludedExpressionIds,
            @JsonProperty("patternMapping") String patternMapping
    ) {
        @JsonCreator
        private TermResultJson {}
    }

    /** Thrown by {@link #parse} on malformed or internally-inconsistent term metadata JSON. */
    public static final class TermMetadataParseException extends RuntimeException {
        public TermMetadataParseException(String message) { super(message); }
        public TermMetadataParseException(String message, Throwable cause) { super(message, cause); }
    }
}
