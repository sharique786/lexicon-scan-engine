package com.db.macs3.ecomms.spectre.decision;

import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.termmeta.ResolvedPatternTree;
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
 * to genuinely verify a NEAR/FOLLOWEDBY/AND/AND-NOT condition for a decomposed
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
 * occurrences are mapped to word indices via {@link #wordSpans}. NEAR
 * allows either direction while FOLLOWEDBY requires strictly increasing
 * indices; gap is the count of whole "words" strictly between two chosen
 * indices. Every satisfying leaf-occurrence combination is enumerated (not
 * just the first), each becoming one output {@link MatchSpan} spanning the
 * earliest-to-latest chosen leaf span — needed because
 * {@code lexicon-hit-summary.term_dtls.regex_match_hit_count} counts every
 * individual occurrence for every other term kind already; collapsing a
 * proximity term to a single synthetic "matched: yes" hit would under-report
 * genuine repeated violations.
 *
 * <h2>CJK: character-based "words," not {@code \S+} tokens</h2>
 * <p>Chinese/Japanese/Korean text has no whitespace between words at all — a
 * whole CJK sentence can be one single {@code \S+} run. Treating that as ONE
 * "word" would be a genuine correctness bug, not just an imprecision: two
 * CJK leaf occurrences landing in the same whitespace-delimited run would
 * both resolve to the SAME word index, making {@code gap} negative (they
 * fail {@code gap >= 0}) and {@code FOLLOWEDBY}'s strict-increase check fail
 * outright — a term whose two halves sit right next to each other in the
 * same sentence, the closest possible proximity, would be reported as NOT
 * matching. {@link #wordSpans} instead gives every individual Han
 * (Chinese/Japanese Kanji)/Hiragana/Katakana/Hangul (Korean) codepoint its
 * OWN word-span, so {@code NEAR{n}}/{@code FOLLOWEDBY{n}}'s {@code n} counts
 * actual CJK CHARACTERS strictly between two matches, exactly as it counts
 * whitespace-delimited tokens for Latin/Cyrillic/etc. text — see that
 * method's Javadoc for why this only needs to special-case CJK scripts.
 * Hebrew needs no special handling here: it IS whitespace-delimited (just
 * right-to-left), and a Java {@code String} always holds LOGICAL character
 * order regardless of visual RTL rendering, so ordinary token counting is
 * already correct for it.
 */
final class ResolvedPatternAreaEvaluator {

    private static final Logger log = LoggerFactory.getLogger(ResolvedPatternAreaEvaluator.class);

    /**
     * Output cap: at most this many distinct occurrence spans reported per area.
     */
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
     * @param tree             the term's parsed resolved-pattern tree
     * @param areaOriginalText ONE scanned area's real original message text (subject,
     *                         message body, or one attachment's clean text) — never
     *                         text from more than one area
     * @return every satisfying occurrence found, capped at {@link #MAX_HITS_PER_AREA} and
     * deduplicated by resulting span — empty if the tree's condition is not
     * satisfied anywhere in this text
     */
    static List<MatchSpan> findMatchingSpans(ResolvedPatternTree tree, String areaOriginalText) {
        if (areaOriginalText == null || areaOriginalText.isBlank()) {
            return List.of();
        }
        if (tree instanceof ResolvedPatternTree.AndNot andNot) {
            List<MatchSpan> excludedSpans = findMatchingSpans(andNot.getExcluded(), areaOriginalText);
            if (!excludedSpans.isEmpty()) {
                return List.of();
            }
            return findMatchingSpans(andNot.getRequired(), areaOriginalText);
        }
        if (tree instanceof ResolvedPatternTree.And and) {
            return matchesAnd(and, areaOriginalText);
        }
        return matchesChain((ResolvedPatternTree.Chain) tree, areaOriginalText);
    }

    /**
     * A plain conjunction: BOTH sides must independently be satisfied somewhere in this SAME
     * area — neither side is an exclusion (contrast {@link ResolvedPatternTree.AndNot} above). When
     * both hold, every occurrence from both sides is reported (deduplicated, capped like every other
     * path here), not just one synthetic "matched" span — same
     * {@code regex_match_hit_count}-completeness reasoning as {@link #matchesChain}.
     */
    private static List<MatchSpan> matchesAnd(ResolvedPatternTree.And and, String areaOriginalText) {
        List<MatchSpan> leftSpans = findMatchingSpans(and.getLeft(), areaOriginalText);
        if (leftSpans.isEmpty()) {
            return List.of();
        }
        List<MatchSpan> rightSpans = findMatchingSpans(and.getRight(), areaOriginalText);
        if (rightSpans.isEmpty()) {
            return List.of();
        }
        Set<MatchSpan> combined = new LinkedHashSet<>(leftSpans);
        combined.addAll(rightSpans);
        List<MatchSpan> result = new ArrayList<>(combined);
        return result.size() > MAX_HITS_PER_AREA ? result.subList(0, MAX_HITS_PER_AREA) : result;
    }

    private static List<MatchSpan> matchesChain(ResolvedPatternTree.Chain chain, String areaOriginalText) {
        List<int[]> words = wordSpans(areaOriginalText);
        List<ResolvedPatternTree> elements = chain.getLeaves();
        List<List<LeafOccurrence>> occurrencesPerElement = new ArrayList<>(elements.size());
        for (ResolvedPatternTree element : elements) {
            List<LeafOccurrence> occurrences = occurrencesFor(element, areaOriginalText, words);
            if (occurrences.isEmpty()) {
                return List.of(); // this element never occurs at all — the whole chain cannot match here
            }
            occurrencesPerElement.add(occurrences);
        }

        Set<MatchSpan> collected = new LinkedHashSet<>();
        int[] visits = {0};
        backtrack(occurrencesPerElement, chain.getOperators(), chain.getDistances(), 0, null,
                new LeafOccurrence[occurrencesPerElement.size()], areaOriginalText, collected, visits);
        if (visits[0] > MAX_BACKTRACK_VISITS) {
            log.debug("resolved-pattern chain evaluation truncated after {} backtracking visits — "
                    + "reporting {} occurrence(s) found so far", visits[0], collected.size());
        }
        return new ArrayList<>(collected);
    }

    /**
     * Every occurrence of one chain element — a flat regex {@link ResolvedPatternTree.Leaf} via
     * {@link #findOccurrences}, or, for a nested {@link ResolvedPatternTree.Chain} element (see that
     * class's Javadoc "Nested chain elements"), every satisfying combination of the INNER group,
     * recursively resolved against this same area text and each converted to a {@link LeafOccurrence}
     * via its own {@link MatchSpan}'s word indices. Either way the outer chain's backtracking measures
     * its own gap the same way — from the near boundary of one occurrence to the near boundary of the
     * next — regardless of whether that occurrence came from a single leaf or a whole inner group.
     */
    private static List<LeafOccurrence> occurrencesFor(ResolvedPatternTree element, String areaOriginalText,
                                                        List<int[]> words) {
        if (element instanceof ResolvedPatternTree.Leaf leaf) {
            return findOccurrences(leaf.getPattern(), areaOriginalText, words);
        }
        List<MatchSpan> nestedSpans = findMatchingSpans(element, areaOriginalText);
        List<LeafOccurrence> occurrences = new ArrayList<>(nestedSpans.size());
        for (MatchSpan span : nestedSpans) {
            int startWordIndex = wordIndexAtOrBefore(words, span.getStartCharIndex() + 1);
            int endWordIndex = wordIndexAtOrBefore(words, span.getEndCharIndex());
            if (startWordIndex >= 0 && endWordIndex >= 0) {
                occurrences.add(new LeafOccurrence(
                        span.getStartCharIndex(), span.getEndCharIndex(), startWordIndex, endWordIndex));
            }
        }
        return occurrences;
    }

    private static void backtrack(List<List<LeafOccurrence>> occurrencesPerLeaf, List<String> operators,
                                  List<Integer> distances, int leafIndex, LeafOccurrence previous,
                                  LeafOccurrence[] chosen, String areaOriginalText,
                                  Set<MatchSpan> collected, int[] visits) {
        if (collected.size() >= MAX_HITS_PER_AREA || visits[0]++ > MAX_BACKTRACK_VISITS) {
            return;
        }
        if (leafIndex == occurrencesPerLeaf.size()) {
            recordMatch(chosen, areaOriginalText, collected);
            return;
        }
        for (LeafOccurrence candidate : occurrencesPerLeaf.get(leafIndex)) {
            chosen[leafIndex] = candidate;
            if (leafIndex == 0 || canExtend(operators.get(leafIndex - 1), distances.get(leafIndex - 1), previous, candidate)) {
                backtrack(occurrencesPerLeaf, operators, distances, leafIndex + 1, candidate, chosen,
                        areaOriginalText, collected, visits);
            }
        }
    }

    private static void recordMatch(LeafOccurrence[] chosen, String areaOriginalText, Set<MatchSpan> collected) {
        int start = chosen[0].startChar();
        int end = chosen[0].endChar();
        for (LeafOccurrence obj : chosen) {
            start = Math.min(start, obj.startChar());
            end = Math.max(end, obj.endChar());
        }
        collected.add(new MatchSpan(start, end, areaOriginalText.substring(start, end)));
    }

    /**
     * @return true iff {@code candidate} legally continues the chain after {@code previous} under
     * {@code operator}'s direction rule and {@code maxGap} — see class Javadoc "NEAR
     * bidirectionality" for why {@code NEAR} never checks direction.
     *
     * <p>Gap is measured between the NEAR boundary of whichever occurrence comes first in the text
     * and the NEAR boundary of whichever comes second — i.e. the earlier occurrence's END word index
     * and the later occurrence's START word index — never using an occurrence's END on both sides.
     * A prior version always compared {@code previous.endWordIndex()} against
     * {@code candidate.endWordIndex()}: for a multi-word leaf (e.g. "mouth shut", "Off shore",
     * "caught red handed") positioned as the later-occurring span, that leaf's OWN internal words got
     * counted as part of the gap (an occurrence's END is {@code (word count - 1)} words to the right
     * of its START), silently rejecting real NEAR{n}/FOLLOWEDBY{n} matches whose true word-gap was
     * within {@code maxGap} — confirmed against a real compiled Compile Service lexicon where every
     * multi-word-leaf proximity term failed to produce a final hit despite Hyperscan's own native
     * COMBINATION correctly reporting all leaves present.
     */
    private static boolean canExtend(String operator, int maxGap, LeafOccurrence previous, LeafOccurrence candidate) {
        boolean previousFirst = previous.endWordIndex() < candidate.startWordIndex();
        boolean candidateFirst = candidate.endWordIndex() < previous.startWordIndex();
        if (!previousFirst && !candidateFirst) {
            return false; // overlapping occurrences — not a valid two-leaf gap
        }
        int gap = previousFirst
                ? candidate.startWordIndex() - previous.endWordIndex() - 1
                : previous.startWordIndex() - candidate.endWordIndex() - 1;
        boolean directionOk = ResolvedPatternTree.OPERATOR_NEAR.equals(operator) || previousFirst;
        return directionOk && gap >= 0 && gap <= maxGap;
    }

    /**
     * Every occurrence of {@code leaf} in {@code areaOriginalText}, carrying its full character span
     * (to synthesize an output {@link MatchSpan}) and the word indices its START and END fall in —
     * both are needed by {@link #canExtend} to measure the gap from whichever boundary of a
     * multi-word leaf actually faces the other occurrence, rather than always its END (see that
     * method's Javadoc for why using END on both sides double-counts a multi-word leaf's own words).
     */
    private static List<LeafOccurrence> findOccurrences(Pattern leaf, String areaOriginalText, List<int[]> words) {
        List<LeafOccurrence> occurrences = new ArrayList<>();
        Matcher matcher = leaf.matcher(areaOriginalText);
        while (matcher.find()) {
            int startWordIndex = wordIndexAtOrBefore(words, matcher.start() + 1);
            int endWordIndex = wordIndexAtOrBefore(words, matcher.end());
            if (startWordIndex >= 0 && endWordIndex >= 0) {
                occurrences.add(new LeafOccurrence(matcher.start(), matcher.end(), startWordIndex, endWordIndex));
            }
            if (matcher.end() == matcher.start()) {
                break; // guard against a zero-width match looping forever
            }
        }
        return occurrences;
    }

    /**
     * Splits {@code text} into "word" spans for gap-counting purposes: a
     * maximal run of non-whitespace, non-CJK codepoints counts as ONE word
     * (the same result {@code \S+} would give), but every individual CJK
     * codepoint (see {@link #isCjkCodePoint}) is its OWN word, one
     * character wide — see class Javadoc "CJK" for why. Iterates by
     * codepoint, not by {@code char}, so a supplementary-plane CJK Extension
     * character (a surrogate pair — rare, but real) is counted as the ONE
     * character it actually is, not two.
     */
    private static List<int[]> wordSpans(String text) {
        List<int[]> spans = new ArrayList<>();
        int length = text.length();
        int tokenStart = -1;
        int index = 0;

        while (index < length) {
            int codePoint = text.codePointAt(index);
            int codePointWidth = Character.charCount(codePoint);
            tokenStart = addSpans(spans, codePoint, tokenStart, index, codePointWidth);
            index += codePointWidth;
        }

        if (tokenStart >= 0) {
            spans.add(new int[]{tokenStart, length});
        }
        return spans;
    }

    private static int addSpans(List<int[]> spans, int codePoint, int tokenStart, int index, int codePointWidth) {
        if (Character.isWhitespace(codePoint)) {
            tokenStart = closeOpenToken(spans, tokenStart, index);
        } else if (isCjkCodePoint(codePoint)) {
            tokenStart = closeOpenToken(spans, tokenStart, index);
            spans.add(new int[]{index, index + codePointWidth});
        } else if (tokenStart < 0) {
            tokenStart = index;
        }

        return tokenStart;
    }

    /**
     * If a word run is currently open ({@code tokenStart >= 0}), closes it as a span ending at
     * {@code endExclusive}. @return the new {@code tokenStart} value (always {@code -1}).
     */
    private static int closeOpenToken(List<int[]> spans, int tokenStart, int endExclusive) {
        if (tokenStart >= 0) {
            spans.add(new int[]{tokenStart, endExclusive});
        }
        return -1;
    }

    /**
     * Han (Chinese characters — also Japanese Kanji), Hiragana/Katakana
     * (Japanese), and Hangul (Korean): the scripts with no whitespace word
     * boundaries, where {@code NEAR}/{@code FOLLOWEDBY} distance must be
     * counted per character — see class Javadoc "CJK." Everything else
     * (Latin, Cyrillic, Hebrew, Arabic, ...) keeps ordinary whitespace-token
     * counting.
     */
    private static boolean isCjkCodePoint(int codePoint) {
        Character.UnicodeScript script = Character.UnicodeScript.of(codePoint);
        return script == Character.UnicodeScript.HAN
                || script == Character.UnicodeScript.HIRAGANA
                || script == Character.UnicodeScript.KATAKANA
                || script == Character.UnicodeScript.HANGUL;
    }

    /**
     * Binary search (not a linear scan) for the LAST word whose start is
     * {@code < charOffset} — {@link #wordSpans} always returns spans in
     * increasing start-offset order, so this is safe. Matters more now than
     * it used to: a long CJK message body's {@code words} list has roughly
     * one entry PER CHARACTER (see class Javadoc "CJK"), not per
     * whitespace-token, so a linear scan here would be O(text length) per
     * leaf occurrence rather than O(log text length).
     */
    private static int wordIndexAtOrBefore(List<int[]> words, int charOffset) {
        int low = 0;
        int high = words.size() - 1;
        int result = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (words.get(mid)[0] < charOffset) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    private record LeafOccurrence(int startChar, int endChar, int startWordIndex, int endWordIndex) {
    }
}
