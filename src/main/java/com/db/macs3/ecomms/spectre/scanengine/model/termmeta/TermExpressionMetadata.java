package com.db.macs3.ecomms.spectre.scanengine.model.termmeta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
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
 * <h2>One TermEntry per term, indexed two ways</h2>
 * <p>{@link #termByAnyExpressionId(int)} maps ANY expression id this
 * feature's {@code .hdb} might report — whether a non-AND-NOT term's own
 * reportable id, or one of an AND NOT term's required/excluded ids — back to
 * the {@link TermEntry} it belongs to. This is the ONLY lookup
 * {@code FeatureScanOrchestrator} needs: given a matched expression id,
 * find its owning term, then evaluate that term's condition using ALL its
 * ids against the complete match set for the message — see
 * {@link TermEntry#requiresExclusionCheck()}.
 *
 * <p>Immutable and safe to cache/share across every message a Spark
 * partition processes for one feature (mirrors
 * {@code HyperscanDatabaseLoader}'s per-partition {@code Database} caching —
 * see {@code TermMetadataLoader}).
 */
public final class TermExpressionMetadata implements Serializable {

    private final String feature;
    private final Map<Integer, TermEntry> byExpressionId;

    private TermExpressionMetadata(String feature, Map<Integer, TermEntry> byExpressionId) {
        this.feature = feature;
        this.byExpressionId = byExpressionId;
    }

    /** @return the term entry owning {@code expressionId}, or null if unrecognised. */
    public TermEntry termByAnyExpressionId(int expressionId) {
        return byExpressionId.get(expressionId);
    }

    /** @return the feature these terms belong to (verbatim {@code body.feature}). */
    public String feature() {
        return feature;
    }

    /** @return how many distinct terms this feature's metadata describes. */
    public int termCount() {
        return (int) byExpressionId.values().stream().map(TermEntry::termNumber).distinct().count();
    }

    /**
     * One term's expression-id shape, resolved from a
     * {@code TermCompilationResult} JSON entry.
     *
     * @param termNumber             parsed from {@code termId}'s {@code ::<n>} suffix —
     *                               what {@code TermIdBuilder.build(feature, termNumber)} needs
     *                               to build the CORRECT {@code term_id} string, regardless of
     *                               which raw expression id actually matched
     * @param termRegexPattern       the required side's pattern text, joined for display —
     *                               used only when the {@code .hdb}'s own embedded pattern text
     *                               (read directly off the {@link com.gliwka.hyperscan.wrapper.Match})
     *                               is unavailable or unreliable, e.g. for an AND NOT term's
     *                               excluded-side match, which must never be surfaced as this
     *                               term's own {@code term_regex_pattern} — see
     *                               {@code FeatureScanOrchestrator}
     * @param requiresExclusionCheck true for AND NOT terms
     * @param requiredExpressionIds  every expression id belonging to this term's required side.
     *                               ALWAYS populated, never empty. Exactly one entry for a
     *                               simple or purely-decomposed (non-AND-NOT) term — that one
     *                               entry IS the term's own reportable id, resolved natively by
     *                               Hyperscan. One entry PER required pattern for an AND NOT term.
     * @param excludedExpressionIds  every expression id belonging to this term's excluded side.
     *                               Null/empty unless {@code requiresExclusionCheck}.
     */
    public record TermEntry(
            int termNumber,
            String termRegexPattern,
            boolean requiresExclusionCheck,
            List<Integer> requiredExpressionIds,
            List<Integer> excludedExpressionIds
    ) {
        /**
         * @return true when this term uses the (still-safe) native Hyperscan
         *         COMBINATION mechanism — pure decomposition, no AND NOT — meaning
         *         its {@link #requiredExpressionIds} has exactly one entry that IS
         *         the term's own reportable id, no further boolean evaluation needed.
         */
        public boolean isNativelyResolved() {
            return !requiresExclusionCheck;
        }
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses one feature's {@code <feature>-compile-results.json} content —
     * the Compile Service's {@code CompileResponse} shape — into an indexed
     * {@link TermExpressionMetadata}.
     *
     * @throws TermMetadataParseException on malformed JSON, a term missing a
     *         parseable {@code ::<n>} term number, or an expression id
     *         appearing under more than one term (both indicate the JSON
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
        if (parsed.results() != null) {
            for (TermResultJson r : parsed.results()) {
                if (!"PASS".equalsIgnoreCase(r.compilationStatus())) {
                    continue; // FAILED terms were never compiled into the .hdb — no ids to index
                }
                int termNumber = parseTermNumber(feature, r.termId());
                String joinedPattern = r.translatedPattern() == null ? null : String.join(" & ", r.translatedPattern());
                boolean requiresExclusion = Boolean.TRUE.equals(r.requiresExclusionCheck());

                TermEntry entry = new TermEntry(
                        termNumber, joinedPattern, requiresExclusion,
                        resolveRequiredIds(feature, r, termNumber),
                        r.excludedExpressionIds());

                for (int id : entry.requiredExpressionIds()) {
                    putUnique(byExpressionId, feature, id, entry);
                }
                if (entry.excludedExpressionIds() != null) {
                    for (int id : entry.excludedExpressionIds()) {
                        putUnique(byExpressionId, feature, id, entry);
                    }
                }
            }
        }
        return new TermExpressionMetadata(feature, byExpressionId);
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

    private static void putUnique(Map<Integer, TermEntry> map, String feature, int id, TermEntry entry) {
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
            @JsonProperty("compilationStatus") String compilationStatus,
            @JsonProperty("translatedPattern") List<String> translatedPattern,
            @JsonProperty("requiresExclusionCheck") Boolean requiresExclusionCheck,
            @JsonProperty("hyperscanExpressionId") Integer hyperscanExpressionId,
            @JsonProperty("requiredExpressionIds") List<Integer> requiredExpressionIds,
            @JsonProperty("excludedExpressionIds") List<Integer> excludedExpressionIds
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
