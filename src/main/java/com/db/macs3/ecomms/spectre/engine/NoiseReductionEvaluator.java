package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.model.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.model.TermMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Evaluates the noise-reduction decision matrix for a given message.
 *
 * <p>The engine always processes noise-reduction features FIRST — identified
 * by {@link FeatureDecisionRow#isNoiseReduction()}, which derives from the
 * raw BQ {@code STRING} column {@code is_noise_reduction} ("Y"/"N") rather
 * than a native boolean.
 *
 * <h2>Decision matrix</h2>
 * <pre>
 * ┌──────────────────┬──────────┬────────────────────────┬────────────────────────────────┐
 * │ Feature Category │ Operator │ Condition               │ Action                         │
 * ├──────────────────┼──────────┼────────────────────────┼────────────────────────────────┤
 * │ Noise Reduction  │ OR       │ ANY feature matches     │ Skip standard features         │
 * │ Noise Reduction  │ OR       │ NONE match              │ Process standard features      │
 * │ Noise Reduction  │ AND      │ ALL features match      │ Skip standard features         │
 * │ Noise Reduction  │ AND      │ NONE/PARTIAL match      │ Process standard features      │
 * │ Only Standard    │ —        │ —                       │ Always process standard        │
 * └──────────────────┴──────────┴────────────────────────┴────────────────────────────────┘
 * </pre>
 *
 * <p>Grouping note: within a single message, multiple {@link FeatureDecisionRow}
 * entries may share the same PARENT composite feature id (one row per lexicon
 * sub-feature). The "feature matched" test in this evaluator operates at the
 * GROUPED level — a parent composite counts as "matched" if ANY of its
 * lexicon sub-features produced a Hyperscan hit (sub-features within one
 * composite are themselves always OR'd together for match purposes; it is
 * the composite-vs-composite relationship that honours the AND/OR operator).
 */
public class NoiseReductionEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(NoiseReductionEvaluator.class);

    private static final String OP_OR  = "OR";
    private static final String OP_AND = "AND";

    /**
     * Determines whether standard lexicon features should be skipped based on
     * the noise-reduction scan results.
     *
     * @param noiseReductionFeatures the (possibly sub-feature-expanded) noise-reduction
     *                               rows for this message — all must have {@code isNoiseReduction() == true}
     * @param scanResults            map of lexiconName -> matches found (empty list = no match)
     * @return {@code true} if standard features should be SKIPPED
     */
    public boolean shouldSkipStandardFeatures(List<FeatureDecisionRow> noiseReductionFeatures,
                                               Map<String, List<TermMatch>> scanResults) {
        if (noiseReductionFeatures == null || noiseReductionFeatures.isEmpty()) {
            log.trace("No noise-reduction features; processing standard features");
            return false;
        }

        // Group by PARENT feature id — a composite's sub-features count as one logical feature
        Map<String, List<FeatureDecisionRow>> byParentFeatureId = noiseReductionFeatures.stream()
                .collect(java.util.stream.Collectors.groupingBy(FeatureDecisionRow::getFeatureId));

        String operator = resolveOperator(noiseReductionFeatures);
        log.debug("Noise-reduction evaluation: {} distinct parent feature(s), operator={}",
                  byParentFeatureId.size(), operator);

        long matchedParentCount = byParentFeatureId.values().stream()
                .filter(subRows -> parentFeatureHasMatch(subRows, scanResults))
                .count();

        boolean skipStandard;
        if (OP_AND.equalsIgnoreCase(operator)) {
            skipStandard = matchedParentCount == byParentFeatureId.size();
            log.debug("NR AND: {}/{} parent features matched -> skipStandard={}",
                      matchedParentCount, byParentFeatureId.size(), skipStandard);
        } else {
            skipStandard = matchedParentCount > 0;
            log.debug("NR OR: {}/{} parent features matched -> skipStandard={}",
                      matchedParentCount, byParentFeatureId.size(), skipStandard);
        }
        return skipStandard;
    }

    /**
     * @return {@code true} if ANY lexicon sub-feature belonging to this parent
     *         feature (a single-element list for direct lexicon features)
     *         produced at least one match.
     */
    private boolean parentFeatureHasMatch(List<FeatureDecisionRow> subRows,
                                           Map<String, List<TermMatch>> scanResults) {
        return subRows.stream().anyMatch(row -> featureHasMatch(row.getLexiconName(), scanResults));
    }

    /**
     * @return {@code true} if the scan results for {@code lexiconName} contain at least one match.
     */
    public boolean featureHasMatch(String lexiconName, Map<String, List<TermMatch>> scanResults) {
        List<TermMatch> matches = scanResults.get(lexiconName);
        return matches != null && !matches.isEmpty();
    }

    /**
     * Determines the operator applied across the (parent-level) noise-reduction
     * features. All noise-reduction features for a message normally share the
     * same {@code feature_operator}; if values differ or are blank, OR is used
     * as the safe default.
     */
    public String resolveOperator(List<FeatureDecisionRow> features) {
        if (features == null || features.isEmpty()) return OP_OR;
        return features.stream()
                .map(FeatureDecisionRow::getFeatureOperator)
                .filter(op -> op != null && !op.isBlank())
                .findFirst()
                .map(String::toUpperCase)
                .orElseGet(() -> {
                    log.warn("No operator found in noise-reduction features; defaulting to OR");
                    return OP_OR;
                });
    }
}
