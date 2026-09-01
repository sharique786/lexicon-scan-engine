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
 * Lexicon Compile Service's {@code CompileResponse}/{@code TermCompilationResult}
 * JSON shape, read from the {@code <feature>-compile-results.json} entry of
 * that feature's zip bundle (alongside its {@code .hdb}).
 *
 * <h2>Expression id scheme</h2>
 * <p>For a simple or purely-decomposed (non-AND-NOT) term,
 * {@code hyperscanExpressionId} is populated and is always the term's own
 * number — Hyperscan's native {@code COMBINATION} mechanism resolves
 * "all decomposed leaves present" on its own, so a matched expression id
 * can be turned directly into a {@code term_id} via
 * {@code TermIdBuilder.build(feature, termNumber)}.
 *
 * <p>For an AND NOT term, {@code hyperscanExpressionId} is null; instead
 * {@code requiredExpressionIds}/{@code excludedExpressionIds} are populated
 * with one ALLOCATED id per pattern, none of which is the term's own
 * number — each compiles as its own plain, individually-reportable
 * expression (never a native {@code COMBINATION}), because Hyperscan
 * evaluates a combination eagerly and progressively: a formula mixing a
 * positive requirement with a negation could otherwise fire before the
 * negated pattern has even been reached by the scan. Resolving a matched
 * expression id back to the correct {@code term_id}, and evaluating the
 * AND NOT boolean condition at all, requires this metadata — the
 * {@code .hdb} file alone is not self-sufficient for AND NOT terms.
 *
 * <h2>{@code resolvedPatterns}: decomposed NEAR/FOLLOWEDBY/AND-NOT terms</h2>
 * <p>A term using {@code NEAR{n}}/{@code FOLLOWEDBY{n}} proximity operators
 * (or, potentially, AND NOT) may be split ("Pattern Too Large") into
 * multiple decomposed {@code regexPattern} leaves (renamed from
 * {@code translatedPattern}), each compiled with the {@code QUIET} Hyperscan
 * flag — meaning Hyperscan's own match callback never reports an individual
 * leaf's matches; only the wrapping native {@code COMBINATION} expression
 * (the term's {@code hyperscanExpressionId}) fires, proving only "every leaf
 * matched somewhere in this scan buffer" — no order/distance information is
 * recoverable from Hyperscan itself for these terms. The
 * {@code resolvedPatterns} field (present iff this decomposition applies)
 * carries the leaves' operator structure as text (e.g.
 * {@code "manipulate NEAR{5} (?:price|spread|stock)"}); see
 * {@link ResolvedPatternTree#build} for how that text's shape is parsed and
 * zipped against the structured {@code regexPattern} leaf list, and
 * {@code ResolvedPatternAreaEvaluator}/{@code FeatureScanOrchestrator} for
 * how the resulting tree is evaluated per scanned area against the message's
 * real original text — the only way to genuinely verify the proximity/
 * AND-NOT condition, since Hyperscan cannot.
 *
 * <p>A term's {@code resolvedPatterns} field (non-blank) is the sole
 * per-term discriminator between this evaluation path and the cross-area,
 * id-presence-only evaluation path above — a term without it (whatever its
 * {@code requiresExclusionCheck} value) uses the id-presence path only.
 *
 * <h2>One TermEntry per term, indexed two ways</h2>
 * <p>{@link #termByAnyExpressionId(int)} maps ANY expression id this
 * feature's {@code .hdb} might report — whether a non-AND-NOT term's own
 * reportable id, or one of an AND NOT term's required/excluded ids — back to
 * the {@link TermEntry} it belongs to. This is the lookup
 * {@code FeatureScanOrchestrator} uses for terms with a matchable expression
 * id. A {@link TermEntry} with {@link TermEntry#requiresPerAreaEvaluation()}
 * true but no expression id at all (a mandatory-per-area AND NOT term the
 * Compile Service gave no id list for) is NOT reachable this way;
 * {@link #mandatoryPerAreaTerms()} is the only way to discover it.
 *
 * <p>Immutable and safe to cache/share across every message a Spark
 * partition processes for one feature.
 */
public final class TermExpressionMetadata implements Serializable {

    private static final String COMPILATION_STATUS_PASS = "PASS";

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
     *                               to build the correct {@code term_id} string, regardless of
     *                               which raw expression id actually matched
     * @param termRegexPattern       the term's pattern text for display — the verbatim
     *                               {@code resolvedPatterns} string when present (preserves the
     *                               NEAR/FOLLOWEDBY operator and distance for analyst readability),
     *                               else the required side's pattern text joined for display
     * @param requiresExclusionCheck true for AND NOT terms
     * @param requiredExpressionIds  every expression id belonging to this term's required side, when
     *                               any exist. Always populated for a term without a
     *                               {@code resolvedPatternTree} — exactly one entry for a
     *                               simple/purely-decomposed term (that one entry IS the term's own
     *                               reportable id), one entry per required pattern for a plain AND
     *                               NOT term. May be null for a term WITH a
     *                               {@code resolvedPatternTree} whose AND NOT shape the Compile
     *                               Service gave no id list for — see {@link #hasCoarseExpressionId()}
     *                               and {@link #mandatoryPerAreaTerms()}
     * @param excludedExpressionIds  every expression id belonging to this term's excluded side.
     *                               Null/empty unless {@code requiresExclusionCheck}.
     * @param resolvedPatternTree    non-null iff this term's JSON entry carried a non-blank
     *                               {@code resolvedPatterns} — see class Javadoc. When non-null,
     *                               {@code FeatureScanOrchestrator} must evaluate this term PER
     *                               SCANNED AREA independently against that area's real original
     *                               text, never merged across areas — word-distance across two
     *                               different texts is meaningless.
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
         * @return true when this term uses the native Hyperscan COMBINATION
         *         mechanism directly — pure decomposition, no AND NOT — meaning
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
            for (TermResultJson termResult : parsed.results()) {
                if (COMPILATION_STATUS_PASS.equalsIgnoreCase(termResult.compilationStatus())) {
                    TermEntry entry = buildTermEntry(feature, termResult);
                    indexTermEntry(feature, entry, byExpressionId, byTermNumber);
                }
                // FAILED terms were never compiled into the .hdb — no ids to index.
            }
        }
        return new TermExpressionMetadata(feature, byExpressionId, byTermNumber);
    }

    private static TermEntry buildTermEntry(String feature, TermResultJson termResult) {
        int termNumber = parseTermNumber(feature, termResult.termId());
        List<String> leaves = patternLeaves(termResult);
        boolean requiresExclusion = Boolean.TRUE.equals(termResult.requiresExclusionCheck());

        ResolvedPatternTree tree = null;
        if (termResult.resolvedPatterns() != null && !termResult.resolvedPatterns().isBlank()) {
            tree = ResolvedPatternTree.build(feature, termResult.termId(), termResult.resolvedPatterns(), leaves);
            validateShapeAgreement(feature, termResult, tree, requiresExclusion);
        }

        String termRegexPattern = tree != null
                ? termResult.resolvedPatterns()
                : (leaves == null ? null : String.join(" & ", leaves));

        RequiredExcludedIds ids = resolveIds(feature, termResult, termNumber, tree);

        return new TermEntry(
                termNumber, termRegexPattern, requiresExclusion, ids.required(), ids.excluded(), tree);
    }

    private record RequiredExcludedIds(List<Integer> required, List<Integer> excluded) {
    }

    /**
     * An AND NOT term with a {@code resolvedPatternTree} takes its ids verbatim from
     * {@code requiredExpressionIds}/{@code excludedExpressionIds} — no throw-if-absent check,
     * since such a term may legitimately have none at all (see {@link #mandatoryPerAreaTerms()}).
     * Every other term (with or without a {@code resolvedPatternTree}) resolves its required id
     * via {@link #resolveRequiredIds}; only a legacy (non-resolvedPatterns) term also carries an
     * excluded id list.
     */
    private static RequiredExcludedIds resolveIds(String feature, TermResultJson termResult, int termNumber,
                                                    ResolvedPatternTree tree) {
        if (tree instanceof ResolvedPatternTree.AndNot) {
            return new RequiredExcludedIds(termResult.requiredExpressionIds(), termResult.excludedExpressionIds());
        }
        List<Integer> requiredIds = resolveRequiredIds(feature, termResult, termNumber);
        List<Integer> excludedIds = tree instanceof ResolvedPatternTree.Chain ? null : termResult.excludedExpressionIds();
        return new RequiredExcludedIds(requiredIds, excludedIds);
    }

    private static void indexTermEntry(String feature, TermEntry entry,
                                        Map<Integer, TermEntry> byExpressionId, Map<Integer, TermEntry> byTermNumber) {
        putUnique(byTermNumber, feature, entry.termNumber(), entry);
        if (entry.requiredExpressionIds() != null) {
            for (int expressionId : entry.requiredExpressionIds()) {
                putUniqueExpressionId(byExpressionId, feature, expressionId, entry);
            }
        }
        if (entry.excludedExpressionIds() != null) {
            for (int expressionId : entry.excludedExpressionIds()) {
                putUniqueExpressionId(byExpressionId, feature, expressionId, entry);
            }
        }
    }

    /** {@code regexPattern} (new schema) if present, else {@code translatedPattern} (old schema). */
    private static List<String> patternLeaves(TermResultJson termResult) {
        return termResult.regexPattern() != null ? termResult.regexPattern() : termResult.translatedPattern();
    }

    /**
     * Cross-checks a {@code resolvedPatterns}-bearing term's shape against
     * its declared {@code requiresExclusionCheck}, {@code hyperscanExpressionId},
     * and {@code patternMapping} siblings — these are all supposed to agree on
     * whether the term is a plain proximity chain or an AND NOT condition. A
     * disagreement means the JSON contradicts the documented id scheme and
     * must surface loudly rather than silently mis-evaluate.
     */
    private static void validateShapeAgreement(String feature, TermResultJson termResult, ResolvedPatternTree tree,
                                                boolean requiresExclusion) {
        boolean isAndNotShape = tree instanceof ResolvedPatternTree.AndNot;
        if (requiresExclusion != isAndNotShape) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.termId() + "' in feature '" + feature + "': requiresExclusionCheck="
                    + requiresExclusion + " does not agree with resolvedPatterns' shape ("
                    + (isAndNotShape ? "AND NOT" : "plain chain") + ") — malformed compile-results JSON.");
        }
        if (isAndNotShape) {
            validateAndNotShapeHasNoNativeCombination(feature, termResult);
        } else {
            validatePatternMappingCount(feature, termResult, (ResolvedPatternTree.Chain) tree);
        }
    }

    /**
     * An AND NOT term must never carry a native {@code hyperscanExpressionId}
     * or {@code patternMapping} — both would mean the {@code .hdb} used
     * native {@code COMBINATION} for this term, contradicting the AND NOT
     * id scheme this class relies on.
     */
    private static void validateAndNotShapeHasNoNativeCombination(String feature, TermResultJson termResult) {
        if (termResult.hyperscanExpressionId() != null) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.termId() + "' in feature '" + feature + "' is an AND NOT term (per "
                    + "resolvedPatterns) but also has a native hyperscanExpressionId="
                    + termResult.hyperscanExpressionId() + " populated — malformed compile-results JSON.");
        }
        if (termResult.patternMapping() != null && !termResult.patternMapping().isBlank()) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.termId() + "' in feature '" + feature + "' is an AND NOT term (per "
                    + "resolvedPatterns) but also has a patternMapping value (" + termResult.patternMapping()
                    + ") populated — malformed compile-results JSON.");
        }
    }

    /**
     * When {@code patternMapping} is present on a plain (non-AND-NOT) chain
     * term, its id count must match the chain's own leaf count — e.g.
     * {@code "(7&8)"} for a 2-leaf chain. Optional field: a chain term with no
     * {@code patternMapping} at all (e.g. a single-leaf term with no
     * decomposition) is not checked.
     */
    private static void validatePatternMappingCount(String feature, TermResultJson termResult, ResolvedPatternTree.Chain chain) {
        String mapping = termResult.patternMapping();
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
                    "Term '" + termResult.termId() + "' in feature '" + feature + "': patternMapping '" + mapping
                    + "' implies " + idCount + " expression id(s) but regexPattern/translatedPattern has "
                    + chain.leaves().size() + " leaf/leaves — malformed compile-results JSON.");
        }
    }

    /**
     * A non-AND-NOT term reports {@code hyperscanExpressionId} (singular);
     * an AND NOT term reports {@code requiredExpressionIds} directly. Either
     * way this returns the required-side id list {@link TermEntry} needs.
     */
    private static List<Integer> resolveRequiredIds(String feature, TermResultJson termResult, int termNumber) {
        if (termResult.requiredExpressionIds() != null && !termResult.requiredExpressionIds().isEmpty()) {
            return termResult.requiredExpressionIds();
        }
        if (termResult.hyperscanExpressionId() != null) {
            return List.of(termResult.hyperscanExpressionId());
        }
        throw new TermMetadataParseException(
                "Term '" + termResult.termId() + "' in feature '" + feature + "' is PASS but has neither "
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
