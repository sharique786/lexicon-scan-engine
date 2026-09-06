package com.db.macs3.ecomms.spectre.scanengine.model.termmeta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <p>Safe to cache/share across every message a Spark partition processes
 * for one feature, PROVIDED nothing mutates a shared instance after
 * publishing it to other threads — this class and {@link TermEntry} are
 * mutable POJOs (setters included), not immutable records; callers must not
 * call a setter on an instance already handed to {@code HyperscanBundleLoader}'s
 * cache.
 */
public class TermExpressionMetadata implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final String COMPILATION_STATUS_PASS = "PASS";

    private String feature;
    private Map<Integer, TermEntry> byExpressionId;
    private Map<Integer, TermEntry> byTermNumber;

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

    /** @return the feature these terms belong to (verbatim {@code body.lexiconName}). */
    public String getFeature() {
        return feature;
    }

    public void setFeature(String feature) {
        this.feature = feature;
    }

    public Map<Integer, TermEntry> getByExpressionId() {
        return byExpressionId;
    }

    public void setByExpressionId(Map<Integer, TermEntry> byExpressionId) {
        this.byExpressionId = byExpressionId;
    }

    public Map<Integer, TermEntry> getByTermNumber() {
        return byTermNumber;
    }

    public void setByTermNumber(Map<Integer, TermEntry> byTermNumber) {
        this.byTermNumber = byTermNumber;
    }

    /** @return how many distinct terms this feature's metadata describes. */
    public int termCount() {
        return byTermNumber.size();
    }

    /**
     * One term's expression-id shape, resolved from a
     * {@code TermCompilationResult} JSON entry.
     */
    public static class TermEntry implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private int termNumber;
        private String termRegexPattern;
        private boolean requiresExclusionCheck;
        private List<Integer> requiredExpressionIds;
        private List<Integer> excludedExpressionIds;
        private ResolvedPatternTree resolvedPatternTree;

        /**
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
         *                               and {@link TermExpressionMetadata#mandatoryPerAreaTerms()}
         * @param excludedExpressionIds  every expression id belonging to this term's excluded side.
         *                               Null/empty unless {@code requiresExclusionCheck}.
         * @param resolvedPatternTree    non-null iff this term's JSON entry carried a non-blank
         *                               {@code resolvedPatterns} — see class Javadoc. When non-null,
         *                               {@code FeatureScanOrchestrator} must evaluate this term PER
         *                               SCANNED AREA independently against that area's real original
         *                               text, never merged across areas — word-distance across two
         *                               different texts is meaningless.
         */
        public TermEntry(int termNumber, String termRegexPattern, boolean requiresExclusionCheck,
                          List<Integer> requiredExpressionIds, List<Integer> excludedExpressionIds,
                          ResolvedPatternTree resolvedPatternTree) {
            this.termNumber = termNumber;
            this.termRegexPattern = termRegexPattern;
            this.requiresExclusionCheck = requiresExclusionCheck;
            this.requiredExpressionIds = requiredExpressionIds;
            this.excludedExpressionIds = excludedExpressionIds;
            this.resolvedPatternTree = resolvedPatternTree;
        }

        public int getTermNumber() { return termNumber; }
        public void setTermNumber(int termNumber) { this.termNumber = termNumber; }
        public String getTermRegexPattern() { return termRegexPattern; }
        public void setTermRegexPattern(String termRegexPattern) { this.termRegexPattern = termRegexPattern; }
        public boolean isRequiresExclusionCheck() { return requiresExclusionCheck; }
        public void setRequiresExclusionCheck(boolean requiresExclusionCheck) { this.requiresExclusionCheck = requiresExclusionCheck; }
        public List<Integer> getRequiredExpressionIds() { return requiredExpressionIds; }
        public void setRequiredExpressionIds(List<Integer> requiredExpressionIds) { this.requiredExpressionIds = requiredExpressionIds; }
        public List<Integer> getExcludedExpressionIds() { return excludedExpressionIds; }
        public void setExcludedExpressionIds(List<Integer> excludedExpressionIds) { this.excludedExpressionIds = excludedExpressionIds; }
        public ResolvedPatternTree getResolvedPatternTree() { return resolvedPatternTree; }
        public void setResolvedPatternTree(ResolvedPatternTree resolvedPatternTree) { this.resolvedPatternTree = resolvedPatternTree; }

        /**
         * @return true when this term uses the native Hyperscan COMBINATION
         *         mechanism directly — pure decomposition, no AND NOT — meaning
         *         its {@link #getRequiredExpressionIds} has exactly one entry that IS
         *         the term's own reportable id, no further boolean evaluation needed.
         *         Always false for a term with a {@link #getResolvedPatternTree} — such
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

        /** @return true iff {@link #getRequiredExpressionIds} gives a usable coarse pre-filter id set. */
        public boolean hasCoarseExpressionId() {
            return requiredExpressionIds != null && !requiredExpressionIds.isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TermEntry)) {
                return false;
            }
            TermEntry other = (TermEntry) o;
            return termNumber == other.termNumber
                    && requiresExclusionCheck == other.requiresExclusionCheck
                    && Objects.equals(termRegexPattern, other.termRegexPattern)
                    && Objects.equals(requiredExpressionIds, other.requiredExpressionIds)
                    && Objects.equals(excludedExpressionIds, other.excludedExpressionIds)
                    && Objects.equals(resolvedPatternTree, other.resolvedPatternTree);
        }

        @Override
        public int hashCode() {
            return Objects.hash(termNumber, termRegexPattern, requiresExclusionCheck, requiredExpressionIds,
                    excludedExpressionIds, resolvedPatternTree);
        }

        @Override
        public String toString() {
            return "TermEntry[termNumber=" + termNumber + ", termRegexPattern=" + termRegexPattern
                    + ", requiresExclusionCheck=" + requiresExclusionCheck
                    + ", requiredExpressionIds=" + requiredExpressionIds
                    + ", excludedExpressionIds=" + excludedExpressionIds
                    + ", resolvedPatternTree=" + resolvedPatternTree + "]";
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
        if (parsed.getResults() != null) {
            for (TermResultJson termResult : parsed.getResults()) {
                if (COMPILATION_STATUS_PASS.equalsIgnoreCase(termResult.getCompilationStatus())) {
                    TermEntry entry = buildTermEntry(feature, termResult);
                    indexTermEntry(feature, entry, byExpressionId, byTermNumber);
                }
                // FAILED terms were never compiled into the .hdb — no ids to index.
            }
        }
        return new TermExpressionMetadata(feature, byExpressionId, byTermNumber);
    }

    private static TermEntry buildTermEntry(String feature, TermResultJson termResult) {
        int termNumber = parseTermNumber(feature, termResult.getTermId());
        List<String> leaves = patternLeaves(termResult);
        boolean requiresExclusion = Boolean.TRUE.equals(termResult.getRequiresExclusionCheck());

        ResolvedPatternTree tree = null;
        if (termResult.getResolvedPatterns() != null && !termResult.getResolvedPatterns().isBlank()) {
            List<String> treeLeaves = withExclusionLeaves(leaves, termResult.getExclusionRegex());
            tree = ResolvedPatternTree.build(feature, termResult.getTermId(), termResult.getResolvedPatterns(), treeLeaves);
            validateShapeAgreement(feature, termResult, tree, requiresExclusion);
        }

        String termRegexPattern = tree != null
                ? termResult.getResolvedPatterns()
                : (leaves == null ? null : String.join(" & ", leaves));

        RequiredExcludedIds ids = resolveIds(feature, termResult, termNumber, tree);

        return new TermEntry(
                termNumber, termRegexPattern, requiresExclusion, ids.getRequired(), ids.getExcluded(), tree);
    }

    private static class RequiredExcludedIds implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private List<Integer> required;
        private List<Integer> excluded;

        RequiredExcludedIds(List<Integer> required, List<Integer> excluded) {
            this.required = required;
            this.excluded = excluded;
        }

        List<Integer> getRequired() { return required; }
        void setRequired(List<Integer> required) { this.required = required; }
        List<Integer> getExcluded() { return excluded; }
        void setExcluded(List<Integer> excluded) { this.excluded = excluded; }
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
            return new RequiredExcludedIds(termResult.getRequiredExpressionIds(), termResult.getExcludedExpressionIds());
        }
        List<Integer> requiredIds = resolveRequiredIds(feature, termResult, termNumber);
        List<Integer> excludedIds = tree instanceof ResolvedPatternTree.Chain ? null : termResult.getExcludedExpressionIds();
        return new RequiredExcludedIds(requiredIds, excludedIds);
    }

    private static void indexTermEntry(String feature, TermEntry entry,
                                        Map<Integer, TermEntry> byExpressionId, Map<Integer, TermEntry> byTermNumber) {
        putUnique(byTermNumber, feature, entry.getTermNumber(), entry);
        if (entry.getRequiredExpressionIds() != null) {
            for (int expressionId : entry.getRequiredExpressionIds()) {
                putUniqueExpressionId(byExpressionId, feature, expressionId, entry);
            }
        }
        if (entry.getExcludedExpressionIds() != null) {
            for (int expressionId : entry.getExcludedExpressionIds()) {
                putUniqueExpressionId(byExpressionId, feature, expressionId, entry);
            }
        }
    }

    /** {@code regexPattern} (new schema) if present, else {@code translatedPattern} (old schema). */
    private static List<String> patternLeaves(TermResultJson termResult) {
        return termResult.getRegexPattern() != null ? termResult.getRegexPattern() : termResult.getTranslatedPattern();
    }

    /**
     * For an AND-NOT-shaped {@code resolvedPatterns} term, the Compile Service reports the required
     * side's leaves in {@code regexPattern}/{@code translatedPattern} and the excluded side's leaf(s)
     * SEPARATELY in {@code exclusionRegex} — confirmed against a real compile-results.json, not
     * documented anywhere before this. {@link ResolvedPatternTree#build} zips {@code resolvedPatterns}'
     * shape against ONE flat leaf list in left-to-right order (required chain first, then the excluded
     * chain), so the two fields must be concatenated in that order before zipping — passing
     * {@code regexPattern} alone leaves the zip cursor short exactly by however many leaves
     * {@code exclusionRegex} was holding.
     */
    private static List<String> withExclusionLeaves(List<String> requiredLeaves, List<String> exclusionLeaves) {
        if (exclusionLeaves == null || exclusionLeaves.isEmpty()) {
            return requiredLeaves;
        }
        List<String> combined = new ArrayList<>();
        if (requiredLeaves != null) {
            combined.addAll(requiredLeaves);
        }
        combined.addAll(exclusionLeaves);
        return combined;
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
                    "Term '" + termResult.getTermId() + "' in feature '" + feature + "': requiresExclusionCheck="
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
     * — that would mean the {@code .hdb} used native {@code COMBINATION} for
     * this term, contradicting the AND NOT id scheme this class relies on.
     *
     * <p>{@code patternMapping} is NOT evidence of native COMBINATION by
     * itself — confirmed against a real compile-results.json, contradicting
     * this class's own earlier (undocumented-elsewhere) assumption that its
     * mere presence implied one: a real AND-NOT-shaped {@code resolvedPatterns}
     * term legitimately carries a {@code patternMapping} such as
     * {@code "((11&12&13)&!14)"} purely as a human-readable rendering of the
     * required/excluded id formula, alongside plain, individually-reportable
     * {@code requiredExpressionIds}/{@code excludedExpressionIds} — not a
     * native combination. When present it is still validated, just against
     * this term's own ids rather than rejected outright.
     */
    private static void validateAndNotShapeHasNoNativeCombination(String feature, TermResultJson termResult) {
        if (termResult.getHyperscanExpressionId() != null) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.getTermId() + "' in feature '" + feature + "' is an AND NOT term (per "
                    + "resolvedPatterns) but also has a native hyperscanExpressionId="
                    + termResult.getHyperscanExpressionId() + " populated — malformed compile-results JSON.");
        }
        validateAndNotPatternMappingMatchesIds(feature, termResult);
    }

    /**
     * When present on an AND-NOT-shaped {@code resolvedPatterns} term,
     * {@code patternMapping}'s ids must be exactly this term's own
     * {@code requiredExpressionIds} ∪ {@code excludedExpressionIds} — the
     * same "trust but verify" treatment {@link #validatePatternMappingCount}
     * gives the plain-chain case, adapted since an AND NOT id formula names
     * ids directly rather than just counting leaves.
     */
    private static void validateAndNotPatternMappingMatchesIds(String feature, TermResultJson termResult) {
        String mapping = termResult.getPatternMapping();
        if (mapping == null || mapping.isBlank()) {
            return;
        }
        Set<Integer> mappingIds = extractIds(mapping);
        Set<Integer> expectedIds = new HashSet<>();
        if (termResult.getRequiredExpressionIds() != null) {
            expectedIds.addAll(termResult.getRequiredExpressionIds());
        }
        if (termResult.getExcludedExpressionIds() != null) {
            expectedIds.addAll(termResult.getExcludedExpressionIds());
        }
        if (!mappingIds.equals(expectedIds)) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.getTermId() + "' in feature '" + feature + "': patternMapping '" + mapping
                    + "' references ids " + mappingIds + " but requiredExpressionIds/excludedExpressionIds give "
                    + expectedIds + " — malformed compile-results JSON.");
        }
    }

    private static final Pattern ID_PATTERN = Pattern.compile("\\d+");

    private static Set<Integer> extractIds(String mapping) {
        Set<Integer> ids = new HashSet<>();
        Matcher matcher = ID_PATTERN.matcher(mapping);
        while (matcher.find()) {
            ids.add(Integer.parseInt(matcher.group()));
        }
        return ids;
    }

    /**
     * When {@code patternMapping} is present on a plain (non-AND-NOT) chain
     * term, its id count must match the chain's own leaf count — e.g.
     * {@code "(7&8)"} for a 2-leaf chain. Optional field: a chain term with no
     * {@code patternMapping} at all (e.g. a single-leaf term with no
     * decomposition) is not checked.
     */
    private static void validatePatternMappingCount(String feature, TermResultJson termResult, ResolvedPatternTree.Chain chain) {
        String mapping = termResult.getPatternMapping();
        if (mapping == null || mapping.isBlank()) {
            return;
        }
        String stripped = mapping.trim();
        if (stripped.startsWith("(") && stripped.endsWith(")")) {
            stripped = stripped.substring(1, stripped.length() - 1);
        }
        int idCount = stripped.isBlank() ? 0 : stripped.split("&").length;
        if (idCount != chain.getLeaves().size()) {
            throw new TermMetadataParseException(
                    "Term '" + termResult.getTermId() + "' in feature '" + feature + "': patternMapping '" + mapping
                    + "' implies " + idCount + " expression id(s) but regexPattern/translatedPattern has "
                    + chain.getLeaves().size() + " leaf/leaves — malformed compile-results JSON.");
        }
    }

    /**
     * A non-AND-NOT term reports {@code hyperscanExpressionId} (singular);
     * an AND NOT term reports {@code requiredExpressionIds} directly. Either
     * way this returns the required-side id list {@link TermEntry} needs.
     */
    private static List<Integer> resolveRequiredIds(String feature, TermResultJson termResult, int termNumber) {
        if (termResult.getRequiredExpressionIds() != null && !termResult.getRequiredExpressionIds().isEmpty()) {
            return termResult.getRequiredExpressionIds();
        }
        if (termResult.getHyperscanExpressionId() != null) {
            return List.of(termResult.getHyperscanExpressionId());
        }
        throw new TermMetadataParseException(
                "Term '" + termResult.getTermId() + "' in feature '" + feature + "' is PASS but has neither "
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
        if (existing != null && existing.getTermNumber() != entry.getTermNumber()) {
            throw new TermMetadataParseException(
                    "Expression id " + id + " in feature '" + feature + "' is claimed by both term "
                    + existing.getTermNumber() + " and term " + entry.getTermNumber()
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
    private static class CompileResponseJson implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private List<TermResultJson> results;

        @JsonCreator
        CompileResponseJson(@JsonProperty("results") List<TermResultJson> results) {
            this.results = results;
        }

        @JsonProperty("results")
        List<TermResultJson> getResults() { return results; }
        void setResults(List<TermResultJson> results) { this.results = results; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TermResultJson implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String termId;
        private String termDescription;
        private String compilationStatus;
        private List<String> translatedPattern;
        private List<String> regexPattern;
        private Boolean requiresExclusionCheck;
        private String resolvedPatterns;
        private List<String> exclusionRegex;
        private Integer hyperscanExpressionId;
        private List<Integer> requiredExpressionIds;
        private List<Integer> excludedExpressionIds;
        private String patternMapping;

        @JsonCreator
        TermResultJson(@JsonProperty("termId") String termId,
                       @JsonProperty("termDescription") String termDescription,
                       @JsonProperty("compilationStatus") String compilationStatus,
                       @JsonProperty("translatedPattern") List<String> translatedPattern,
                       @JsonProperty("regexPattern") List<String> regexPattern,
                       @JsonProperty("requiresExclusionCheck") Boolean requiresExclusionCheck,
                       @JsonProperty("resolvedPatterns") String resolvedPatterns,
                       @JsonProperty("exclusionRegex") List<String> exclusionRegex,
                       @JsonProperty("hyperscanExpressionId") Integer hyperscanExpressionId,
                       @JsonProperty("requiredExpressionIds") List<Integer> requiredExpressionIds,
                       @JsonProperty("excludedExpressionIds") List<Integer> excludedExpressionIds,
                       @JsonProperty("patternMapping") String patternMapping) {
            this.termId = termId;
            this.termDescription = termDescription;
            this.compilationStatus = compilationStatus;
            this.translatedPattern = translatedPattern;
            this.regexPattern = regexPattern;
            this.requiresExclusionCheck = requiresExclusionCheck;
            this.resolvedPatterns = resolvedPatterns;
            this.exclusionRegex = exclusionRegex;
            this.hyperscanExpressionId = hyperscanExpressionId;
            this.requiredExpressionIds = requiredExpressionIds;
            this.excludedExpressionIds = excludedExpressionIds;
            this.patternMapping = patternMapping;
        }

        @JsonProperty("termId")
        String getTermId() { return termId; }
        void setTermId(String termId) { this.termId = termId; }
        @JsonProperty("termDescription")
        String getTermDescription() { return termDescription; }
        void setTermDescription(String termDescription) { this.termDescription = termDescription; }
        @JsonProperty("compilationStatus")
        String getCompilationStatus() { return compilationStatus; }
        void setCompilationStatus(String compilationStatus) { this.compilationStatus = compilationStatus; }
        @JsonProperty("translatedPattern")
        List<String> getTranslatedPattern() { return translatedPattern; }
        void setTranslatedPattern(List<String> translatedPattern) { this.translatedPattern = translatedPattern; }
        @JsonProperty("regexPattern")
        List<String> getRegexPattern() { return regexPattern; }
        void setRegexPattern(List<String> regexPattern) { this.regexPattern = regexPattern; }
        @JsonProperty("requiresExclusionCheck")
        Boolean getRequiresExclusionCheck() { return requiresExclusionCheck; }
        void setRequiresExclusionCheck(Boolean requiresExclusionCheck) { this.requiresExclusionCheck = requiresExclusionCheck; }
        @JsonProperty("resolvedPatterns")
        String getResolvedPatterns() { return resolvedPatterns; }
        void setResolvedPatterns(String resolvedPatterns) { this.resolvedPatterns = resolvedPatterns; }
        @JsonProperty("exclusionRegex")
        List<String> getExclusionRegex() { return exclusionRegex; }
        void setExclusionRegex(List<String> exclusionRegex) { this.exclusionRegex = exclusionRegex; }
        @JsonProperty("hyperscanExpressionId")
        Integer getHyperscanExpressionId() { return hyperscanExpressionId; }
        void setHyperscanExpressionId(Integer hyperscanExpressionId) { this.hyperscanExpressionId = hyperscanExpressionId; }
        @JsonProperty("requiredExpressionIds")
        List<Integer> getRequiredExpressionIds() { return requiredExpressionIds; }
        void setRequiredExpressionIds(List<Integer> requiredExpressionIds) { this.requiredExpressionIds = requiredExpressionIds; }
        @JsonProperty("excludedExpressionIds")
        List<Integer> getExcludedExpressionIds() { return excludedExpressionIds; }
        void setExcludedExpressionIds(List<Integer> excludedExpressionIds) { this.excludedExpressionIds = excludedExpressionIds; }
        @JsonProperty("patternMapping")
        String getPatternMapping() { return patternMapping; }
        void setPatternMapping(String patternMapping) { this.patternMapping = patternMapping; }
    }

    /** Thrown by {@link #parse} on malformed or internally-inconsistent term metadata JSON. */
    public static final class TermMetadataParseException extends RuntimeException {
        public TermMetadataParseException(String message) { super(message); }
        public TermMetadataParseException(String message, Throwable cause) { super(message, cause); }
    }
}
