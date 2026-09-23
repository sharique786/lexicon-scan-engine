package com.db.macs3.ecomms.spectre.decision;

import com.db.macs3.ecomms.spectre.model.match.MatchSpan;
import com.db.macs3.ecomms.spectre.model.termmeta.ResolvedPatternTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link ResolvedPatternAreaEvaluator#findMatchingSpans}'s
 * NEAR/FOLLOWEDBY/AND-NOT gap-distance logic directly — package-private
 * access, no need to go through a real Hyperscan compile (see
 * {@code FeatureScanOrchestratorTest} for the heavier, end-to-end
 * resolvedPatterns coverage). This class is specifically about
 * {@link ResolvedPatternAreaEvaluator}'s "word" segmentation across scripts:
 * whitespace-delimited (Latin, Hebrew, Arabic) vs. character-delimited
 * (CJK — see that class's Javadoc "CJK" section for why Chinese/Japanese/
 * Korean text needs per-CHARACTER distance counting, not per-{@code \S+}-
 * token) — plus {@code NearBidirectionality}, which confirms {@code NEAR}
 * (unlike {@code FOLLOWEDBY}) is genuinely order-agnostic for BOTH
 * token-based and character-based scripts, not just "usually written
 * forward."
 */
@DisplayName("ResolvedPatternAreaEvaluator")
class ResolvedPatternAreaEvaluatorTest {

    private static Pattern leaf(String text) {
        return Pattern.compile(text, ResolvedPatternTree.JAVA_LEAF_FLAGS);
    }

    private static ResolvedPatternTree.Leaf leafNode(String text) {
        return new ResolvedPatternTree.Leaf(leaf(text));
    }

    private static ResolvedPatternTree.Chain singleLeafChain(String text) {
        return new ResolvedPatternTree.Chain(List.of(leafNode(text)), List.of(), List.of());
    }

    private static ResolvedPatternTree.Chain twoLeafChain(String leaf1, String operator, int distance, String leaf2) {
        return new ResolvedPatternTree.Chain(
                List.of(leafNode(leaf1), leafNode(leaf2)), List.of(operator), List.of(distance));
    }

    private static ResolvedPatternTree.Chain threeLeafChain(String leaf1, String op1, int dist1,
                                                            String leaf2, String op2, int dist2, String leaf3) {
        return new ResolvedPatternTree.Chain(List.of(leafNode(leaf1), leafNode(leaf2), leafNode(leaf3)),
                List.of(op1, op2), List.of(dist1, dist2));
    }

    private static List<MatchSpan> evaluate(ResolvedPatternTree tree, String text) {
        return ResolvedPatternAreaEvaluator.findMatchingSpans(tree, text);
    }

    @Nested
    @DisplayName("Latin (baseline — whitespace-token distance, unchanged behavior)")
    class Latin {

        @Test
        @DisplayName("NEAR{n} matches when exactly n whole words separate the two leaves")
        void nearMatchesAtExactGap() {
            // "manipulate" (word 0) ... "price" (word 3) — 2 words (the, stock) strictly between.
            String text = "manipulate the stock price today";
            assertThat(evaluate(twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 2, "price"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 1, "price"), text))
                    .isEmpty();
        }

        @Test
        @DisplayName("FOLLOWEDBY requires strictly increasing order; NEAR does not")
        void followedByRequiresOrder() {
            String text = "price of the manipulate";
            // "price" occurs BEFORE "manipulate" here, so FOLLOWEDBY(manipulate, price) must fail.
            assertThat(evaluate(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "price"), text))
                    .isEmpty();
            assertThat(evaluate(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 2, "price"), text))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("Chinese (Han script — no whitespace, must count characters)")
    class Chinese {

        // 甲(0) 乙(1) 丙(2) 丁(3) 戊(4) — five single-character "terms" with zero separators.
        private static final String FIVE_CHARACTERS = "甲乙丙丁戊";

        @Test
        @DisplayName("NEAR{n} counts CJK CHARACTERS strictly between two matches, not whitespace tokens")
        void nearCountsCharactersNotTokens() {
            // Exactly 2 characters (乙, 丙) sit strictly between 甲 and 丁.
            assertThat(evaluate(twoLeafChain("甲", ResolvedPatternTree.OPERATOR_NEAR, 2, "丁"), FIVE_CHARACTERS))
                    .containsExactly(new MatchSpan(0, 4, "甲乙丙丁"));
            assertThat(evaluate(twoLeafChain("甲", ResolvedPatternTree.OPERATOR_NEAR, 1, "丁"), FIVE_CHARACTERS))
                    .isEmpty();
        }

        @Test
        @DisplayName("without CJK-aware segmentation this would incorrectly reject NEAR{0} for adjacent characters")
        void nearZeroMatchesTrulyAdjacentCharacters() {
            // 甲 and 乙 are directly adjacent (no character between them at all) — this is the exact
            // case a whitespace-only \S+ split would break: both would land in the SAME "word" (the
            // whole run), making gap negative and failing `gap >= 0` even though nothing could be
            // closer than this.
            assertThat(evaluate(twoLeafChain("甲", ResolvedPatternTree.OPERATOR_NEAR, 0, "乙"), FIVE_CHARACTERS))
                    .hasSize(1);
        }

        @Test
        @DisplayName("FOLLOWEDBY respects character order within a CJK run, exactly like whitespace-token text")
        void followedByRespectsCharacterOrder() {
            assertThat(evaluate(
                    twoLeafChain("甲", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "丁"), FIVE_CHARACTERS))
                    .hasSize(1);
            // Reversed leaf order relative to where the characters actually occur — 丁 (index 3)
            // does not precede 甲 (index 0), so FOLLOWEDBY must fail even though NEAR would succeed.
            assertThat(evaluate(
                    twoLeafChain("丁", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "甲"), FIVE_CHARACTERS))
                    .isEmpty();
            assertThat(evaluate(
                    twoLeafChain("丁", ResolvedPatternTree.OPERATOR_NEAR, 2, "甲"), FIVE_CHARACTERS))
                    .hasSize(1);
        }

        @Test
        @DisplayName("a multi-character leaf's gap is measured from its NEAR boundary, not double-counting its own characters")
        void multiCharacterLeafLandsOnLastCharacter() {
            // 市場 (chars 0-1) ... 隠蔽 (chars 4-5): 操 and 作 (indices 2,3) are strictly between the
            // end of the first leaf and the start of the second — 2 characters of gap.
            String text = "市場操作隠蔽";
            assertThat(evaluate(twoLeafChain("市場", ResolvedPatternTree.OPERATOR_NEAR, 2, "隠蔽"), text))
                    .containsExactly(new MatchSpan(0, 6, "市場操作隠蔽"));
            assertThat(evaluate(twoLeafChain("市場", ResolvedPatternTree.OPERATOR_NEAR, 1, "隠蔽"), text))
                    .isEmpty();
        }

        @Test
        @DisplayName("AND NOT: an excluded CJK leaf present anywhere in the area blocks the whole match")
        void andNotBlocksOnExcludedCjkLeaf() {
            ResolvedPatternTree andNot = new ResolvedPatternTree.AndNot(
                    singleLeafChain("股票"), singleLeafChain("操纵"));

            assertThat(evaluate(andNot, "股票操纵")).isEmpty(); // excluded term present
            assertThat(evaluate(andNot, "股票上涨")).hasSize(1); // excluded term absent, required present
        }

        @Test
        @DisplayName("a three-leaf CJK chain enumerates two independent gaps in one pass")
        void threeLeafChainMatchesBothGaps() {
            // 甲(0) 乙(1) 丙(2) 丁(3) 戊(4): gap(甲,丙)=1, gap(丙,戊)=1.
            ResolvedPatternTree.Chain chain = threeLeafChain(
                    "甲", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 1,
                    "丙", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 1, "戊");
            assertThat(evaluate(chain, FIVE_CHARACTERS)).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Japanese (Kanji + Hiragana + Katakana mixed in one run, all treated as CJK)")
    class Japanese {

        @Test
        @DisplayName("Katakana/Hiragana/Kanji characters in the same contiguous run are each their own word")
        void mixedScriptRunCountsEveryCharacter() {
            // リスク(katakana, 0-2) を(hiragana, 3) 検討(kanji, 4-5) する(hiragana, 6-7) — one
            // unbroken run with no whitespace across three different Unicode scripts.
            String text = "リスクを検討する";
            // Only を (index 3) sits strictly between the end of リスク (index 2) and the start of
            // 検討 (index 4) — one character of gap; 検 itself is part of the second leaf, not the gap.
            assertThat(evaluate(twoLeafChain("リスク", ResolvedPatternTree.OPERATOR_NEAR, 1, "検討"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("リスク", ResolvedPatternTree.OPERATOR_NEAR, 0, "検討"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Korean (Hangul — also character-delimited, no whitespace within a compound term)")
    class Korean {

        @Test
        @DisplayName("NEAR{n} counts Hangul syllable characters strictly between two matches")
        void nearCountsHangulCharacters() {
            // 주식(0-1) 시장(2-3) 조작(4-5) — "stock market manipulation" with no spaces.
            // 시 and 장 (indices 2,3) are strictly between the end of 주식 and the start of 조작.
            String text = "주식시장조작";
            assertThat(evaluate(twoLeafChain("주식", ResolvedPatternTree.OPERATOR_NEAR, 2, "조작"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("주식", ResolvedPatternTree.OPERATOR_NEAR, 1, "조작"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Mixed CJK + Latin text in the same area")
    class MixedScript {

        @Test
        @DisplayName("a whitespace-delimited Latin word between two CJK terms counts as exactly one word")
        void latinWordBetweenCjkTermsCountsAsOneWord() {
            // "report" is the sole whitespace-delimited word strictly between 股票 and 操纵 — one
            // word of gap, exactly as it would count for pure-Latin text either side.
            String text = "股票 report 操纵";
            assertThat(evaluate(twoLeafChain("股票", ResolvedPatternTree.OPERATOR_NEAR, 1, "操纵"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("股票", ResolvedPatternTree.OPERATOR_NEAR, 0, "操纵"), text))
                    .isEmpty();
            assertThat(evaluate(twoLeafChain("股票", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 1, "操纵"), text))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("NEAR is bidirectional — swapping which leaf comes first in the chain doesn't change the result")
    class NearBidirectionality {

        @Test
        @DisplayName("token-based (Latin): A NEAR B and B NEAR A over the same text match IDENTICALLY")
        void latinNearIsOrderSymmetric() {
            String text = "manipulate the stock price today";
            List<MatchSpan> forward =
                    evaluate(twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 2, "price"), text);
            List<MatchSpan> reversed =
                    evaluate(twoLeafChain("price", ResolvedPatternTree.OPERATOR_NEAR, 2, "manipulate"), text);
            assertThat(forward).hasSize(1);
            assertThat(reversed).isEqualTo(forward);
        }

        @Test
        @DisplayName("character-based (Chinese): A NEAR B and B NEAR A over the same text match IDENTICALLY")
        void chineseNearIsOrderSymmetric() {
            String text = "甲乙丙丁戊";
            List<MatchSpan> forward = evaluate(twoLeafChain("甲", ResolvedPatternTree.OPERATOR_NEAR, 2, "丁"), text);
            List<MatchSpan> reversed = evaluate(twoLeafChain("丁", ResolvedPatternTree.OPERATOR_NEAR, 2, "甲"), text);
            assertThat(forward).hasSize(1);
            assertThat(reversed).isEqualTo(forward);
        }

        @Test
        @DisplayName("token-based RTL (Arabic): A NEAR B and B NEAR A over the same text match IDENTICALLY")
        void arabicNearIsOrderSymmetric() {
            String text = "واحد اثنان ثلاثة أربعة"; // one two three four
            List<MatchSpan> forward =
                    evaluate(twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_NEAR, 2, "أربعة"), text);
            List<MatchSpan> reversed =
                    evaluate(twoLeafChain("أربعة", ResolvedPatternTree.OPERATOR_NEAR, 2, "واحد"), text);
            assertThat(forward).hasSize(1);
            assertThat(reversed).isEqualTo(forward);
        }

        @Test
        @DisplayName("NEAR matches even when the leaves are written in the OPPOSITE of reading order")
        void nearMatchesRegardlessOfWhichTermOccursFirstInTheText() {
            // "price" occurs textually BEFORE "manipulate" here — FOLLOWEDBY(manipulate, price)
            // would fail this (see Latin.followedByRequiresOrder), but NEAR must not care.
            assertThat(evaluate(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 2, "price"),
                    "price of the manipulate"))
                    .hasSize(1);
            // Same property for a CJK character pair.
            assertThat(evaluate(
                    twoLeafChain("丁", ResolvedPatternTree.OPERATOR_NEAR, 2, "甲"), "甲乙丙丁戊"))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("Hebrew (right-to-left, but whitespace-delimited like Latin)")
    class Hebrew {

        @Test
        @DisplayName("NEAR{n} counts whitespace-delimited WORDS, not characters — same convention as Latin")
        void nearCountsWordsNotCharacters() {
            // "aleph beta gamma delta" — two whole words (בטא, גמא) strictly between אלפא and דלתא.
            // If Hebrew were (incorrectly) treated as character-delimited like CJK, this gap would
            // come out as roughly 10 (individual letters) instead of 2 (words).
            String text = "אלפא בטא גמא דלתא";
            assertThat(evaluate(twoLeafChain("אלפא", ResolvedPatternTree.OPERATOR_NEAR, 2, "דלתא"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("אלפא", ResolvedPatternTree.OPERATOR_NEAR, 1, "דלתא"), text))
                    .isEmpty();
        }

        @Test
        @DisplayName("a Java String holds Hebrew in LOGICAL order, so FOLLOWEDBY's left-to-right reading order still applies")
        void followedByUsesLogicalNotVisualOrder() {
            String text = "אלפא בטא גמא דלתא";
            assertThat(evaluate(
                    twoLeafChain("אלפא", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "דלתא"), text))
                    .hasSize(1);
            assertThat(evaluate(
                    twoLeafChain("דלתא", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "אלפא"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Arabic (right-to-left, whitespace-delimited like Hebrew — NOT CJK)")
    class Arabic {

        // "one two three four" — four whitespace-delimited words, no CJK-script codepoints.
        private static final String FOUR_WORDS = "واحد اثنان ثلاثة أربعة";

        @Test
        @DisplayName("NEAR{n} counts whitespace-delimited WORDS, not characters — same convention as Hebrew/Latin")
        void nearCountsWordsNotCharacters() {
            // Two whole words (اثنان, ثلاثة) strictly between واحد and أربعة. If Arabic were
            // (incorrectly) treated as character-delimited like CJK, this gap would come out as
            // individual letters instead of 2 words.
            assertThat(evaluate(twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_NEAR, 2, "أربعة"), FOUR_WORDS))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_NEAR, 1, "أربعة"), FOUR_WORDS))
                    .isEmpty();
        }

        @Test
        @DisplayName("a Java String holds Arabic in LOGICAL order, so FOLLOWEDBY's left-to-right reading order still applies")
        void followedByUsesLogicalNotVisualOrder() {
            assertThat(evaluate(
                    twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "أربعة"), FOUR_WORDS))
                    .hasSize(1);
            assertThat(evaluate(
                    twoLeafChain("أربعة", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "واحد"), FOUR_WORDS))
                    .isEmpty();
        }

        @Test
        @DisplayName("a combining diacritic (tashkeel) embedded mid-word does not fragment word-index counting")
        void diacriticsDoNotFragmentWordBoundaries() {
            // سُوق ("market") carries an embedded combining Damma diacritic (U+064F) — still ONE
            // non-whitespace token overall, exactly as an accented Latin word would be; it must not
            // be miscounted as extra words (nor, since it isn't HAN/HIRAGANA/KATAKANA/HANGUL, split
            // into individual per-character words the way CJK is).
            String text = "واحد سُوق أربعة";
            assertThat(evaluate(twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_NEAR, 1, "أربعة"), text))
                    .hasSize(1);
            assertThat(evaluate(twoLeafChain("واحد", ResolvedPatternTree.OPERATOR_NEAR, 0, "أربعة"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Supplementary-plane CJK characters (surrogate pairs)")
    class SupplementaryPlane {

        @Test
        @DisplayName("a CJK Extension B character (outside the BMP) counts as ONE character, not two")
        void supplementaryCharacterCountsAsOneWord() {
            // U+20000 (𠀀), a real CJK Unified Ideographs Extension B codepoint, encoded as a
            // surrogate pair in UTF-16 — constructed via Character.toChars rather than a literal to
            // make the exact codepoint under test unambiguous regardless of file/editor encoding.
            String supplementary = new String(Character.toChars(0x20000));
            String text = supplementary + "乙" + "丁";
            // supplementary(1 codepoint, 2 chars) 乙(1 char, strictly between) 丁(1 char) — if the
            // surrogate pair were miscounted as TWO separate "characters" instead of one, the gap
            // computed below would come out as 0 instead of 1, for every such character in the text.
            assertThat(evaluate(
                    twoLeafChain(Pattern.quote(supplementary), ResolvedPatternTree.OPERATOR_NEAR, 1, "丁"), text))
                    .hasSize(1);
            assertThat(evaluate(
                    twoLeafChain(Pattern.quote(supplementary), ResolvedPatternTree.OPERATOR_NEAR, 0, "丁"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Plain AND — a genuine conjunction, unlike AND NOT (regression: lexicon_research_3::2 shape)")
    class PlainAnd {

        @Test
        @DisplayName("both sides present in the area: matches, reporting spans from BOTH sides")
        void bothSidesPresent_matches() {
            ResolvedPatternTree and = new ResolvedPatternTree.And(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 5, "price"),
                    singleLeafChain("disclosure"));

            List<MatchSpan> spans = evaluate(and, "we manipulate the closing price after the disclosure");
            assertThat(spans).hasSize(2);
        }

        @Test
        @DisplayName("only the left (NEAR-chain) side present: does NOT match — unlike AND NOT, the right "
                     + "side must be PRESENT, not absent")
        void onlyLeftSidePresent_doesNotMatch() {
            ResolvedPatternTree and = new ResolvedPatternTree.And(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 5, "price"),
                    singleLeafChain("disclosure"));

            assertThat(evaluate(and, "we manipulate the closing price today")).isEmpty();
        }

        @Test
        @DisplayName("only the right (single-leaf) side present: does NOT match")
        void onlyRightSidePresent_doesNotMatch() {
            ResolvedPatternTree and = new ResolvedPatternTree.And(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 5, "price"),
                    singleLeafChain("disclosure"));

            assertThat(evaluate(and, "nothing but the disclosure here")).isEmpty();
        }

        @Test
        @DisplayName("left side's own condition (the NEAR gap) still applies inside an And — too far apart "
                     + "on the left fails the whole And even when the right leaf is present")
        void leftSideNearGapStillEnforcedInsideAnd() {
            ResolvedPatternTree and = new ResolvedPatternTree.And(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 1, "price"),
                    singleLeafChain("disclosure"));

            // "manipulate" and "price" are 4 words apart here — fails NEAR{1} — even though
            // "disclosure" (the right side) is present.
            assertThat(evaluate(and, "manipulate one two three price and also disclosure")).isEmpty();
        }

        @Test
        @DisplayName("neither side present: does not match")
        void neitherSidePresent_doesNotMatch() {
            ResolvedPatternTree and = new ResolvedPatternTree.And(
                    twoLeafChain("manipulate", ResolvedPatternTree.OPERATOR_NEAR, 5, "price"),
                    singleLeafChain("disclosure"));

            assertThat(evaluate(and, "completely unrelated text")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Multi-word-leaf production examples (regression: lexicon_research_1::9/10 shape, "
            + "reported 2026-09-23)")
    class MultiWordLeafProductionExamples {

        // Both leaves and text below are taken verbatim from lexicon_research_1's real
        // compile-results.json (terms ::9 and ::10) and the two message texts reported as
        // "not matching." Re-verified end-to-end against the REAL .hdb/JSON via
        // FeatureScanOrchestrator (not just this class) — both terms genuinely hit. This is the
        // exact bug class the v3.10 near-boundary gap fix (see canExtend's own Javadoc, which
        // names "mouth shut" and "Off shore" as its own worked examples) already covers; these
        // tests exist to pin that confirmation down permanently, not because a new defect was
        // found.

        @Test
        @DisplayName("keep FOLLOWEDBY{3} mouth shut — gap is exactly 3 (in, the, market)")
        void keepFollowedByMouthShut() {
            String text = "Second-line review retained keep in the market mouth shut after the syndicate "
                    + "update because the wording needs assessment.";
            assertThat(evaluate(
                    twoLeafChain("keep", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 3, "mouth shut"), text))
                    .hasSize(1);
            // One word narrower than the true gap must still fail — confirms this isn't
            // passing merely because the gap check was accidentally skipped.
            assertThat(evaluate(
                    twoLeafChain("keep", ResolvedPatternTree.OPERATOR_FOLLOWEDBY, 2, "mouth shut"), text))
                    .isEmpty();
        }

        @Test
        @DisplayName("Off shore NEAR{2} account — leaves occur in reverse (account before Off shore), "
                + "gap is exactly 2 (during, trading)")
        void offShoreNearAccount() {
            String text = "The case note highlights account during trading Off shore before the closing "
                    + "discussion for follow-up by the surveillance team.";
            assertThat(evaluate(
                    twoLeafChain("Off shore", ResolvedPatternTree.OPERATOR_NEAR, 2, "account"), text))
                    .hasSize(1);
            assertThat(evaluate(
                    twoLeafChain("Off shore", ResolvedPatternTree.OPERATOR_NEAR, 1, "account"), text))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Nested chain elements — a chain element that is itself a proximity chain "
            + "(regression: lexicon_research_1::26 shape)")
    class NestedChainElements {

        // "manipulate" NEAR{n} (an inner "price" NEAR{5} "stock" chain) — mirrors the real
        // Compile Service shape "(manipulate) NEAR{5} ((price) NEAR{5} (stock))".
        private static ResolvedPatternTree.Chain outerChain(int outerDistance) {
            return new ResolvedPatternTree.Chain(
                    List.of(leafNode("manipulate"), twoLeafChain("price", ResolvedPatternTree.OPERATOR_NEAR, 5, "stock")),
                    List.of(ResolvedPatternTree.OPERATOR_NEAR), List.of(outerDistance));
        }

        @Test
        @DisplayName("matches when both the outer gap AND the inner group's own gap are satisfied")
        void matchesWhenBothOuterAndInnerGapsSatisfied() {
            // manipulate(0) the(1) closing(2) price(3) of(4) the(5) stock(6) market(7):
            // inner gap(price,stock)=2 <=5; outer gap(manipulate, nested span)=2 <=5.
            String text = "manipulate the closing price of the stock market";
            assertThat(evaluate(outerChain(5), text)).hasSize(1);
        }

        @Test
        @DisplayName("fails when the OUTER gap is too large, even though the inner group is satisfied on its own")
        void failsWhenOuterGapTooLarge() {
            String text = "manipulate the closing price of the stock market";
            assertThat(evaluate(outerChain(1), text)).isEmpty();
        }

        @Test
        @DisplayName("fails when the INNER group's own condition is not satisfied, regardless of the outer gap")
        void failsWhenInnerGroupConditionNotSatisfied() {
            // price(2) ... stock(14): inner gap = 11, exceeds the inner chain's own NEAR{5} — the
            // nested element has NO satisfying occurrence at all, so the outer chain cannot match
            // here even with a generous outer distance.
            String text = "manipulate the price of one two three four five six seven eight nine ten stock";
            assertThat(evaluate(outerChain(50), text)).isEmpty();
        }

        @Test
        @DisplayName("fails outright when the outer leaf is entirely absent, even though the inner group matches")
        void failsWhenOuterLeafAbsent() {
            String text = "the closing price of the stock market";
            assertThat(evaluate(outerChain(5), text)).isEmpty();
        }
    }
}
