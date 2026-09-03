package com.db.macs3.ecomms.spectre.scanengine.decision;

import com.db.macs3.ecomms.spectre.scanengine.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.scanengine.model.termmeta.ResolvedPatternTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluates one {@link ResolvedPatternTree} against ONE scanned area's real
 * original message text, using plain {@code java.util.regex} — the only way
 * to genuinely verify a NEAR/FOLLOWEDBY/AND-NOT condition for a decomposed
 * term, since the {@code .hdb}'s decomposed leaves are compiled QUIET (see
 * {@code TermExpressionMetadata} class Javadoc): Hyperscan's own native
 * {@code COMBINATION} match only proves "every leaf matched somewhere in
 * this scan buffer," never the leaves' relative order or word-distance.
 *
 * <h2>Per-area only — never merge across areas</h2>
 * <p>Word-distance/order is only meaningful within one contiguous text. A
 * required word in the subject and an excluded word in the body do NOT
 * share a coordinate space for a proximity/AND-NOT condition evaluated
 * here — {@code FeatureScanOrchestrator} must call this once per scanned
 * area, independently, never on text merged/concatenated across areas.
 *
 * <h2>Algorithm</h2>
 * <p>Each leaf is matched via {@code Matcher.find()} against the real text;
 * occurrences are mapped to word indices via {@code \S+} word spans. NEAR
 * allows either direction while FOLLOWEDBY requires strictly increasing
 * indices; gap is the count of whole words strictly between two chosen
 * indices. Every satisfying leaf-occurrence combination is enumerated (not
 * just the first), each becoming one output {@link MatchSpan} spanning the
 * earliest-to-latest chosen leaf span — needed because
 * {@code lexicon-hit-summary.term_dtls.regex_match_hit_count} counts every
 * individual occurrence for every other term kind already; collapsing a
 * proximity term to a single synthetic "matched: yes" hit would under-report
 * genuine repeated violations.
 */
final class ResolvedPatternAreaEvaluator {

    private static final Logger log = LoggerFactory.getLogger(ResolvedPatternAreaEvaluator.class);

    /** Output cap: at most this many distinct occurrence spans reported per area. */
    private static final int MAX_HITS_PER_AREA = 50;

    /**
     * Internal safety bound on backtracking work — deliberately much larger
     * than {@link #MAX_HITS_PER_AREA}, so a pathological leaf-occurrence
     * count (many leaves, each with many occurrences) degrades by
     * truncation, logged once, rather than by hanging or failing the
     * message/partition.
     */
    private static final int MAX_BACKTRACK_VISITS = 200_000;

    private ResolvedPatternAreaEvaluator() {
    }

    /**
     * @param tree               the term's parsed resolved-pattern tree
     * @param areaOriginalText   ONE scanned area's real original message text (subject,
     *                           message body, or one attachment's clean text) — never
     *                           text from more than one area
     * @return every satisfying occurrence found, capped at {@link #MAX_HITS_PER_AREA} and
     *         deduplicated by resulting span — empty if the tree's condition is not
     *         satisfied anywhere in this text
     */
    static List<MatchSpan> findMatchingSpans(ResolvedPatternTree tree, String areaOriginalText) {
        if (areaOriginalText == null || areaOriginalText.isBlank()) {
            return List.of();
        }
        if (tree instanceof ResolvedPatternTree.AndNot andNot) {
            List<MatchSpan> excludedSpans = findMatchingSpans(andNot.excluded(), areaOriginalText);
            if (!excludedSpans.isEmpty()) {
                return List.of();
            }
            return findMatchingSpans(andNot.required(), areaOriginalText);
        }
        return matchesChain((ResolvedPatternTree.Chain) tree, areaOriginalText);
    }

    private static List<MatchSpan> matchesChain(ResolvedPatternTree.Chain chain, String areaOriginalText) {
        List<int[]> words = wordSpans(areaOriginalText);
        List<List<LeafOccurrence>> occurrencesPerLeaf = new ArrayList<>(chain.leaves().size());
        for (Pattern leaf : chain.leaves()) {
            List<LeafOccurrence> occurrences = findOccurrences(leaf, areaOriginalText, words);
            if (occurrences.isEmpty()) {
                return List.of(); // this leaf never appears at all — the whole chain cannot match here
            }
            occurrencesPerLeaf.add(occurrences);
        }

        Set<MatchSpan> collected = new LinkedHashSet<>();
        int[] visits = {0};
        backtrack(occurrencesPerLeaf, chain.operators(), chain.distances(), 0, null,
                new LeafOccurrence[occurrencesPerLeaf.size()], areaOriginalText, collected, visits);
        if (visits[0] > MAX_BACKTRACK_VISITS) {
            log.debug("resolved-pattern chain evaluation truncated after {} backtracking visits — "
                    + "reporting {} occurrence(s) found so far", visits[0], collected.size());
        }
        return new ArrayList<>(collected);
    }

    private static void backtrack(List<List<LeafOccurrence>> occurrencesPerLeaf, List<String> operators,
                                   List<Integer> distances, int leafIndex, LeafOccurrence previous,
                                   LeafOccurrence[] chosen, String areaOriginalText,
                                   Set<MatchSpan> collected, int[] visits) {
        if (collected.size() >= MAX_HITS_PER_AREA || visits[0]++ > MAX_BACKTRACK_VISITS) {
            return;
        }
        if (leafIndex == occurrencesPerLeaf.size()) {
            int start = chosen[0].startChar();
            int end = chosen[0].endChar();
            for (LeafOccurrence o : chosen) {
                start = Math.min(start, o.startChar());
                end = Math.max(end, o.endChar());
            }
            collected.add(new MatchSpan(start, end, areaOriginalText.substring(start, end)));
            return;
        }
        for (LeafOccurrence candidate : occurrencesPerLeaf.get(leafIndex)) {
            chosen[leafIndex] = candidate;
            if (leafIndex == 0) {
                backtrack(occurrencesPerLeaf, operators, distances, leafIndex + 1, candidate, chosen,
                        areaOriginalText, collected, visits);
                continue;
            }
            String operator = operators.get(leafIndex - 1);
            int maxGap = distances.get(leafIndex - 1);
            boolean directionOk = ResolvedPatternTree.OPERATOR_NEAR.equals(operator)
                    || candidate.endWordIndex() > previous.endWordIndex();
            int gap = Math.abs(candidate.endWordIndex() - previous.endWordIndex()) - 1;
            if (directionOk && gap >= 0 && gap <= maxGap) {
                backtrack(occurrencesPerLeaf, operators, distances, leafIndex + 1, candidate, chosen,
                        areaOriginalText, collected, visits);
            }
        }
    }

    /**
     * Every occurrence of {@code leaf} in {@code areaOriginalText}, carrying
     * both its full character span (to synthesize an output {@link MatchSpan})
     * and the word index its END falls in (for the same gap-counting
     * definition {@code NEAR{n}}/{@code FOLLOWEDBY{n}} use elsewhere in this
     * platform).
     */
    private static List<LeafOccurrence> findOccurrences(Pattern leaf, String areaOriginalText, List<int[]> words) {
        List<LeafOccurrence> occurrences = new ArrayList<>();
        Matcher m = leaf.matcher(areaOriginalText);
        while (m.find()) {
            int endWordIndex = wordIndexAtOrBefore(words, m.end());
            if (endWordIndex >= 0) {
                occurrences.add(new LeafOccurrence(m.start(), m.end(), endWordIndex));
            }
            if (m.end() == m.start()) {
                break; // guard against a zero-width match looping forever
            }
        }
        return occurrences;
    }

    private static List<int[]> wordSpans(String text) {
        List<int[]> spans = new ArrayList<>();
        Matcher m = Pattern.compile("\\S+").matcher(text);
        while (m.find()) {
            spans.add(new int[] {m.start(), m.end()});
        }
        return spans;
    }

    private static int wordIndexAtOrBefore(List<int[]> words, int charOffset) {
        for (int wordIndex = words.size() - 1; wordIndex >= 0; wordIndex--) {
            if (words.get(wordIndex)[0] < charOffset) {
                return wordIndex;
            }
        }
        return -1;
    }

    private record LeafOccurrence(int startChar, int endChar, int endWordIndex) {
    }
}
