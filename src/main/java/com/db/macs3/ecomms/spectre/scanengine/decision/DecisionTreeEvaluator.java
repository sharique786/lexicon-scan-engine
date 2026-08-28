package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.GroupEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.match.AreaMatch;
import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchArea;
import com.db.macs3.ecomms.spectre.scanengine.model.match.TermMatchResult;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map;

/**
 * Evaluates one message's ordered {@link FeatureGroup}s (see
 * {@code FeatureGroupingService}) against the decision tree — requirement 2's
 * NoiseReduction → Disclaimer → Lexicon ordering, the noise-reduction
 * short-circuit rule, and disclaimer-precedence suppression.
 *
 * <h2>Noise reduction: evaluated group-by-group, short-circuits on first hit</h2>
 * <p>Per the confirmed answer to "how do multiple separate noise-reduction
 * {@code featureId}s combine": each NoiseReduction-category group is
 * evaluated independently, using ONLY its own {@link FeatureGroup#operator()}
 * across its own members (requirement 2.g's matrix) — there is no additional
 * combination operator ACROSS different {@code featureId}s. Groups are
 * evaluated in order; the moment any one of them is a hit, evaluation stops
 * immediately — no further NoiseReduction group, the Disclaimer group, and
 * every Lexicon group are simply never evaluated, not evaluated-and-ignored.
 *
 * <h2>Disclaimer: a precedence lexicon, not the Scanner Service's detector</h2>
 * <p>Per requirement 8.a, disclaimer here is a Hyperscan-scanned feature like
 * any other lexicon (ordinary {@code .hdb}, ordinary scan pass) — NOT the
 * Lexicon Scanner Service's separate exact-substring disclaimer detector.
 * It is simply processed before standard Lexicon groups (requirement 2.f),
 * and its matches are used afterward to suppress overlapping Lexicon
 * matches (requirement 2.j) — full containment only (requirement 8.b: a
 * Lexicon match is suppressed only when ENTIRELY inside a disclaimer
 * match's span, never on partial overlap), and only within the SAME area
 * (subject-vs-subject, body-vs-body, or the SAME attachment's content —
 * comparing raw indices across different areas would be meaningless, since
 * each area is its own independent coordinate space).
 */
public final class DecisionTreeEvaluator {

    /**
     * Scans one {@link FeatureDecisionRow} (one lexicon feature applied to
     * one message) and returns every term match found, across whatever
     * areas that row's {@code feature_definition.body.scope} covers. Left
     * as a caller-supplied function so decision-tree ORDERING/SHORT-CIRCUIT/
     * SUPPRESSION logic can be tested independently of Hyperscan/GCS — see
     * {@code FeatureScanOrchestrator} for the real, Spark-facing implementation.
     */
    @FunctionalInterface
    public interface FeatureRowScanner {
        List<TermMatchResult> scan(FeatureDecisionRow row);
    }

    private DecisionTreeEvaluator() {}

    /**
     * @param messageId      the message being evaluated, for the result's identity
     * @param orderedGroups   from {@code FeatureGroupingService.groupAndOrder} — MUST already
     *                         be in NoiseReduction → Disclaimer → Lexicon order; this method
     *                         does not re-sort
     * @param scanner          scans one row on demand
     */
    public static MessageEvaluationResult evaluate(String messageId, List<FeatureGroup> orderedGroups,
                                                     FeatureRowScanner scanner) {
        List<GroupEvaluationResult> evaluated = new ArrayList<>();
        List<TermMatchResult> disclaimerMatches = new ArrayList<>();

        for (FeatureGroup group : orderedGroups) {
            GroupEvaluationResult result = evaluateGroup(group, scanner);
            evaluated.add(result);

            if (group.isNoiseReduction()) {
                if (result.isHit()) {
                    // Short-circuit: every later group (further NoiseReduction, Disclaimer,
                    // all Lexicon) is never evaluated at all.
                    return new MessageEvaluationResult(messageId, evaluated, true, List.of(), Map.of(), 0);
                }
                continue;
            }

            if (group.isDisclaimer()) {
                disclaimerMatches.addAll(flatten(result));
            }
        }

        List<AreaMatch> disclaimerSpans = new ArrayList<>();
        for (TermMatchResult tmr : disclaimerMatches) {
            disclaimerSpans.addAll(tmr.matches());
        }

        Map<String, List<TermMatchResult>> finalByFeatureId = new LinkedHashMap<>();
        int totalSuppressed = 0;
        for (GroupEvaluationResult result : evaluated) {
            if (result.group().isNoiseReduction() || result.group().isDisclaimer()) {
                continue;
            }
            Suppression suppression = suppressDisclaimerOverlaps(flatten(result), disclaimerSpans);
            totalSuppressed += suppression.suppressedCount();
            if (!suppression.kept().isEmpty()) {
                finalByFeatureId.put(result.group().featureId(), suppression.kept());
            }
        }

        return new MessageEvaluationResult(
                messageId, evaluated, false, disclaimerMatches, finalByFeatureId, totalSuppressed);
    }

    // ── Group evaluation ─────────────────────────────────────────────────────

    private static GroupEvaluationResult evaluateGroup(FeatureGroup group, FeatureRowScanner scanner) {
        Map<FeatureDecisionRow, List<TermMatchResult>> memberMatches = new LinkedHashMap<>();
        Map<FeatureDecisionRow, Boolean> memberHit = new LinkedHashMap<>();

        for (FeatureDecisionRow member : group.members()) {
            List<TermMatchResult> matches = scanner.scan(member);
            matches = matches == null ? List.of() : matches;
            memberMatches.put(member, matches);
            // minimumHits is informational only for this engine (confirmed) — any match at
            // all, for any term within this member's lexicon, counts as a hit.
            memberHit.put(member, !matches.isEmpty());
        }

        boolean groupHit = resolveGroupHit(group, memberHit);
        return new GroupEvaluationResult(group, memberMatches, memberHit, groupHit);
    }

    /**
     * Resolves a group's overall hit status from its members' individual hit
     * statuses — single-member groups need no operator (that one member's
     * hit status IS the group's); multi-member groups apply
     * {@link FeatureGroup#operator()} (confirmed semantics: OR = any member
     * hit, AND = every member hit).
     */
    private static boolean resolveGroupHit(FeatureGroup group, Map<FeatureDecisionRow, Boolean> memberHit) {
        if (!group.isMultiMember()) {
            return memberHit.values().iterator().next();
        }
        boolean isOr = BqColumns.OPERATOR_OR.equalsIgnoreCase(group.operator());
        boolean isAnd = BqColumns.OPERATOR_AND.equalsIgnoreCase(group.operator());
        if (!isOr && !isAnd) {
            throw new IllegalStateException(
                    "featureId=" + group.featureId() + " has an unrecognised operator: '" + group.operator()
                    + "' — expected OR or AND.");
        }
        if (isOr) {
            return memberHit.values().stream().anyMatch(Boolean::booleanValue);
        }
        return memberHit.values().stream().allMatch(Boolean::booleanValue);
    }

    private static List<TermMatchResult> flatten(GroupEvaluationResult result) {
        List<TermMatchResult> all = new ArrayList<>();
        for (List<TermMatchResult> perMember : result.memberMatches().values()) {
            all.addAll(perMember);
        }
        return all;
    }

    // ── Disclaimer-overlap suppression ──────────────────────────────────────

    private static final class Suppression {
        private final List<TermMatchResult> kept;
        private final int suppressedCount;

        Suppression(List<TermMatchResult> kept, int suppressedCount) {
            this.kept = kept;
            this.suppressedCount = suppressedCount;
        }

        List<TermMatchResult> kept() { return kept; }
        int suppressedCount() { return suppressedCount; }
    }

    /**
     * Removes any Lexicon-category {@link AreaMatch} that is FULLY CONTAINED
     * (requirement 8.b) within a disclaimer match's span, comparing only
     * within the SAME area (and, for {@code ATTACHMENT}, the same
     * {@code attachmentId} — see class Javadoc). A {@link TermMatchResult}
     * left with zero surviving matches after suppression is dropped
     * entirely, since {@link TermMatchResult} requires at least one match.
     */
    private static Suppression suppressDisclaimerOverlaps(List<TermMatchResult> rawGroupMatches,
                                                            List<AreaMatch> disclaimerSpans) {
        if (disclaimerSpans.isEmpty()) {
            return new Suppression(rawGroupMatches, 0);
        }

        List<TermMatchResult> kept = new ArrayList<>();
        int suppressedCount = 0;

        for (TermMatchResult tmr : rawGroupMatches) {
            List<AreaMatch> survivingMatches = new ArrayList<>();
            for (AreaMatch am : tmr.matches()) {
                boolean suppressed = disclaimerSpans.stream().anyMatch(d -> sameAreaScope(am, d)
                        && am.span().isFullyContainedIn(d.span()));
                if (suppressed) {
                    suppressedCount++;
                } else {
                    survivingMatches.add(am);
                }
            }
            if (!survivingMatches.isEmpty()) {
                kept.add(new TermMatchResult(tmr.termId(), tmr.termRegexPattern(), survivingMatches));
            }
        }

        return new Suppression(kept, suppressedCount);
    }

    /** True iff two matches are in the same coordinate space — same area, and same attachment if the area is ATTACHMENT. */
    private static boolean sameAreaScope(AreaMatch a, AreaMatch b) {
        if (a.area() != b.area()) {
            return false;
        }
        if (a.area() == MatchArea.ATTACHMENT) {
            return a.attachmentId().equals(b.attachmentId());
        }
        return true;
    }
}
