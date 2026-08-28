package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups one message's {@link FeatureDecisionRow}s (all sharing one
 * {@code messageId}) by {@code featureId} into {@link FeatureGroup}s, in the
 * order {@code DecisionTreeEvaluator} must process them: NoiseReduction
 * groups first, then Disclaimer, then everything else (requirement 2.f).
 *
 * <p>This class is pure/Spark-agnostic — it operates on plain Java lists, so
 * it can be unit tested directly and called identically whether the caller
 * obtained the per-message row list via a Spark {@code groupByKey}/
 * {@code cogroup} or by any other means.
 */
public final class FeatureGroupingService {

    private FeatureGroupingService() {}

    /**
     * Groups {@code rowsForOneMessage} by {@code featureId} and orders the
     * resulting groups for decision-tree processing.
     *
     * @param rowsForOneMessage every {@link FeatureDecisionRow} for a single
     *                           {@code messageId} — behaviour is undefined
     *                           (rows will be silently mixed) if rows for more
     *                           than one message are passed together
     * @return groups in processing order: every {@code is_noise_reduction=Y}
     *          group first (in the order their {@code featureId} first
     *          appeared in {@code rowsForOneMessage}), then every
     *          {@code disclaimer}-type group, then every remaining group
     * @throws IllegalArgumentException if a {@code featureId}'s member rows
     *                                   disagree on {@code featureType} or
     *                                   {@code isNoiseReduction} — this would
     *                                   indicate a data-quality problem in the
     *                                   view itself, surfaced loudly rather
     *                                   than silently resolved by picking one
     */
    public static List<FeatureGroup> groupAndOrder(List<FeatureDecisionRow> rowsForOneMessage) {
        if (rowsForOneMessage == null || rowsForOneMessage.isEmpty()) {
            return List.of();
        }

        // LinkedHashMap preserves first-seen featureId order, which is what determines
        // relative ordering WITHIN a processing category below.
        Map<String, List<FeatureDecisionRow>> byFeatureId = new LinkedHashMap<>();
        for (FeatureDecisionRow row : rowsForOneMessage) {
            byFeatureId.computeIfAbsent(row.featureId(), k -> new ArrayList<>()).add(row);
        }

        List<FeatureGroup> groups = new ArrayList<>(byFeatureId.size());
        for (Map.Entry<String, List<FeatureDecisionRow>> entry : byFeatureId.entrySet()) {
            groups.add(buildGroup(entry.getKey(), entry.getValue()));
        }

        groups.sort(Comparator.comparingInt(FeatureGroupingService::processingCategory));
        return groups;
    }

    private static FeatureGroup buildGroup(String featureId, List<FeatureDecisionRow> members) {
        FeatureDecisionRow first = members.get(0);
        validateConsistency(featureId, members, first);

        String operator = members.size() > 1 ? first.operator() : null;
        if (members.size() > 1 && (operator == null || operator.isBlank())) {
            throw new IllegalArgumentException(
                    "featureId=" + featureId + " has " + members.size()
                    + " member rows but no operator value — an operator is required to combine "
                    + "multiple sub-features (see requirement 2.g's decision matrix).");
        }

        return new FeatureGroup(
                featureId,
                first.featureName(),
                first.featureType(),
                first.isNoiseReductionFlag(),
                operator,
                members);
    }

    private static void validateConsistency(String featureId, List<FeatureDecisionRow> members, FeatureDecisionRow first) {
        for (FeatureDecisionRow row : members) {
            if (!eq(row.featureType(), first.featureType())) {
                throw new IllegalArgumentException(
                        "featureId=" + featureId + " has inconsistent featureType across its member rows: '"
                        + first.featureType() + "' vs '" + row.featureType() + "'");
            }
            if (!eq(row.isNoiseReduction(), first.isNoiseReduction())) {
                throw new IllegalArgumentException(
                        "featureId=" + featureId + " has inconsistent is_noise_reduction across its member rows: '"
                        + first.isNoiseReduction() + "' vs '" + row.isNoiseReduction() + "'");
            }
            if (members.size() > 1 && !eq(row.operator(), first.operator())) {
                throw new IllegalArgumentException(
                        "featureId=" + featureId + " has inconsistent operator across its member rows: '"
                        + first.operator() + "' vs '" + row.operator() + "'");
            }
        }
    }

    private static boolean eq(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    /** Lower sorts first: 0 = NoiseReduction, 1 = Disclaimer, 2 = everything else (standard Lexicon/Composite). */
    private static int processingCategory(FeatureGroup group) {
        if (group.isNoiseReduction()) {
            return 0;
        }
        if (group.isDisclaimer()) {
            return 1;
        }
        return 2;
    }
}
