package com.db.macs3.ecomms.spectre.model.termmeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The evaluable form of one term's {@code resolvedPatterns} string — a
 * decomposed representation of a NEAR/FOLLOWEDBY proximity chain, a plain AND
 * conjunction, and/or an AND NOT condition, kept as a left-to-right sequence
 * of {@code regexPattern} leaves joined by the operator text
 * {@code resolvedPatterns} carries. See {@code ResolvedPatternAreaEvaluator}
 * for how a tree is evaluated against real message text, and
 * {@code TermExpressionMetadata} class Javadoc for why this exists
 * (decomposed leaves are compiled QUIET in the {@code .hdb} — Hyperscan
 * itself can never report their individual positions, so the real
 * proximity/AND/AND-NOT condition must be re-verified in Java against the
 * leaves' own regex text).
 *
 * <h2>Plain AND vs. AND NOT — a genuinely separate operator</h2>
 * <p>A real term has been observed combining a {@code NEAR{n}}/
 * {@code FOLLOWEDBY{n}} chain with a further, independently-required
 * condition via a plain {@code " AND ("} — e.g.
 * {@code "leaf1 NEAR{5} leaf2 AND leaf3"} — distinct from
 * {@code requiresExclusionCheck}'s {@code " AND NOT ("} shape: BOTH sides
 * must be present (see {@link And}), never an exclusion. {@link #parseShape}
 * checks {@link #AND_NOT_MARKER} first — a term is never both shapes at once
 * in the data observed so far.
 *
 * <h2>Shape parsing, not text slicing</h2>
 * <p>{@link #build} never slices leaf regex text directly out of the
 * {@code resolvedPatterns} string — doing so would be unsafe if a leaf's own
 * regex text happened to contain a literal operator marker like
 * {@code " NEAR{5} "}. Instead, paren-depth-aware scanning discovers only the
 * tree's SHAPE (leaf count per chain segment, operator+distance sequence,
 * the AND/AND-NOT split point), which is then zipped against the {@code regexPattern}
 * list positionally, in the same left-to-right order {@code resolvedPatterns}
 * renders them in. A leaf-count disagreement between the two fields becomes a
 * structural parse error (the zip cursor running out, or having leftovers)
 * rather than a silent mismatch.
 *
 * <h2>{@link Chain}/{@link And}/{@link AndNot} are mutable POJOs, marked {@code non-sealed}</h2>
 * <p>Java requires every direct permitted subtype of a {@code sealed}
 * interface to be {@code final}, {@code sealed}, or {@code non-sealed}. This
 * project's model-package convention (mutable POJOs, no {@code final}) rules
 * out {@code final}, so {@link Chain}/{@link And}/{@link AndNot} (and
 * {@link ShapeNode}'s {@code ChainShape}/{@code AndShape}/{@code AndNotShape})
 * are {@code non-sealed} instead — the sealed CONTRACT on
 * {@link ResolvedPatternTree}/{@link ShapeNode} themselves (exhaustive
 * {@code instanceof}/{@code switch} over a closed set) is unaffected, since
 * this file is still the only place new implementations can be declared.
 *
 * <h2>Not {@code Serializable} — deliberately</h2>
 * <p>{@link Pattern} does not implement {@link java.io.Serializable}, so
 * neither can {@link Chain}. This is safe only because {@code HyperscanBundleLoader}
 * is constructed INSIDE {@code PartitionProcessor}'s {@code mapPartitions}
 * closure body — every {@code TermExpressionMetadata}/{@code TermEntry}/
 * {@code ResolvedPatternTree} is built fresh, executor-local, per partition,
 * and never serialized across the wire. If a future refactor ever moves that
 * construction to the driver and ships the loaded object via closure
 * capture, this assumption breaks with a {@code NotSerializableException} —
 * fix the refactor, not by making this class Serializable.
 */
public sealed interface ResolvedPatternTree {

    /**
     * A (possibly length-1, i.e. no proximity operator at all) sequence of
     * leaves connected by NEAR/FOLLOWEDBY.
     */
    non-sealed class Chain implements ResolvedPatternTree {

        private List<Pattern> leaves;
        private List<String> operators;
        private List<Integer> distances;

        /**
         * @param leaves    one compiled, case-insensitive pattern per leaf, in left-to-right term order
         * @param operators {@code "NEAR"} or {@code "FOLLOWEDBY"} between consecutive leaves —
         *                  {@code operators.size() == leaves.size() - 1}
         * @param distances the raw distance for each operator — same size as {@code operators}
         */
        public Chain(List<Pattern> leaves, List<String> operators, List<Integer> distances) {
            this.leaves = leaves;
            this.operators = operators;
            this.distances = distances;
        }

        public List<Pattern> getLeaves() {
            return leaves;
        }

        public void setLeaves(List<Pattern> leaves) {
            this.leaves = leaves;
        }

        public List<String> getOperators() {
            return operators;
        }

        public void setOperators(List<String> operators) {
            this.operators = operators;
        }

        public List<Integer> getDistances() {
            return distances;
        }

        public void setDistances(List<Integer> distances) {
            this.distances = distances;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            Chain other = (Chain) obj;
            return Objects.equals(leaves, other.leaves)
                    && Objects.equals(operators, other.operators)
                    && Objects.equals(distances, other.distances);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leaves, operators, distances);
        }

        @Override
        public String toString() {
            return "Chain[leaves=" + leaves + ", operators=" + operators + ", distances=" + distances + "]";
        }
    }

    /**
     * left/right — a plain CONJUNCTION: both sides must independently be satisfied within the SAME
     * area for this node to match. Unlike {@link AndNot}, neither side is an exclusion — this is
     * {@code "(chain A) AND (chain B)"}, not {@code "(chain A) AND NOT (chain B)"}. Confirmed real
     * shape: a {@code NEAR{n}}/{@code FOLLOWEDBY{n}} chain plainly ANDed with a further single-leaf
     * (or, unconfirmed, chain) condition, e.g. {@code "leaf1 NEAR{5} leaf2 AND leaf3"} — both
     * {@code left} and {@code right} are always a plain {@link Chain} today (mirroring
     * {@link AndNot}'s "never nested" contract); a term combining a plain AND with an AND NOT in the
     * same {@code resolvedPatterns} string has not been observed and is not handled — see
     * {@link #parseShape}.
     */
    non-sealed class And implements ResolvedPatternTree {

        private ResolvedPatternTree left;
        private ResolvedPatternTree right;

        public And(ResolvedPatternTree left, ResolvedPatternTree right) {
            this.left = left;
            this.right = right;
        }

        public ResolvedPatternTree getLeft() {
            return left;
        }

        public void setLeft(ResolvedPatternTree left) {
            this.left = left;
        }

        public ResolvedPatternTree getRight() {
            return right;
        }

        public void setRight(ResolvedPatternTree right) {
            this.right = right;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            And other = (And) obj;
            return Objects.equals(left, other.left) && Objects.equals(right, other.right);
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }

        @Override
        public String toString() {
            return "And[left=" + left + ", right=" + right + "]";
        }
    }

    /**
     * required/excluded — {@code required} is the required side, always a plain {@link Chain}, never a
     * further {@link AndNot} (AND NOT is never nested, per the Compile Service's own documented
     * contract); {@code excluded} is the excluded side, always a plain {@link Chain}.
     */
    non-sealed class AndNot implements ResolvedPatternTree {

        private ResolvedPatternTree required;
        private ResolvedPatternTree excluded;

        public AndNot(ResolvedPatternTree required, ResolvedPatternTree excluded) {
            this.required = required;
            this.excluded = excluded;
        }

        public ResolvedPatternTree getRequired() {
            return required;
        }

        public void setRequired(ResolvedPatternTree required) {
            this.required = required;
        }

        public ResolvedPatternTree getExcluded() {
            return excluded;
        }

        public void setExcluded(ResolvedPatternTree excluded) {
            this.excluded = excluded;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            AndNot other = (AndNot) obj;
            return Objects.equals(required, other.required) && Objects.equals(excluded, other.excluded);
        }

        @Override
        public int hashCode() {
            return Objects.hash(required, excluded);
        }

        @Override
        public String toString() {
            return "AndNot[required=" + required + ", excluded=" + excluded + "]";
        }
    }

    int JAVA_LEAF_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS;

    /**
     * {@link Chain#getOperators()} values.
     */
    String OPERATOR_NEAR = "NEAR";
    String OPERATOR_FOLLOWEDBY = "FOLLOWEDBY";

    /**
     * Parses {@code resolvedPatterns}'s SHAPE (never its leaf text — see
     * class Javadoc) and zips it against {@code regexPatternLeaves} in
     * left-to-right order to build an evaluable tree.
     *
     * @param feature            for error messages only
     * @param termId             for error messages only
     * @param resolvedPatterns   the exact {@code resolvedPatterns} string from the compile-results JSON
     * @param regexPatternLeaves the exact {@code regexPattern} (or {@code translatedPattern}) list —
     *                           must contain exactly as many entries as {@code resolvedPatterns}'s
     *                           shape has leaves, in the same order
     * @throws TermExpressionMetadata.TermMetadataParseException on any structural mismatch
     */
    static ResolvedPatternTree build(String feature, String termId, String resolvedPatterns,
                                     List<String> regexPatternLeaves) {
        if (regexPatternLeaves == null || regexPatternLeaves.isEmpty()) {
            throw new TermExpressionMetadata.TermMetadataParseException(
                    "Term '" + termId + "' in feature '" + feature + "' has a resolvedPatterns value but no "
                            + "regexPattern/translatedPattern leaves to zip it against.");
        }
        ShapeNode shape = parseShape(feature, termId, resolvedPatterns.trim());
        Iterator<String> cursor = regexPatternLeaves.iterator();
        ResolvedPatternTree tree = zip(feature, termId, shape, cursor);
        if (cursor.hasNext()) {
            throw new TermExpressionMetadata.TermMetadataParseException(
                    "Term '" + termId + "' in feature '" + feature + "': regexPattern has more leaves than "
                            + "resolvedPatterns' shape implies (" + regexPatternLeaves.size() + " provided).");
        }
        return tree;
    }

    // ── Shape discovery (adapted from the ResolvedPatternMatcher reference — shape only, no leaf text) ──

    sealed interface ShapeNode {
        non-sealed class ChainShape implements ShapeNode {

            private int leafCount;
            private List<String> operators;
            private List<Integer> distances;

            ChainShape(int leafCount, List<String> operators, List<Integer> distances) {
                this.leafCount = leafCount;
                this.operators = operators;
                this.distances = distances;
            }

            int getLeafCount() {
                return leafCount;
            }

            void setLeafCount(int leafCount) {
                this.leafCount = leafCount;
            }

            List<String> getOperators() {
                return operators;
            }

            void setOperators(List<String> operators) {
                this.operators = operators;
            }

            List<Integer> getDistances() {
                return distances;
            }

            void setDistances(List<Integer> distances) {
                this.distances = distances;
            }
        }

        non-sealed class AndNotShape implements ShapeNode {

            private ShapeNode required;
            private ShapeNode excluded;

            AndNotShape(ShapeNode required, ShapeNode excluded) {
                this.required = required;
                this.excluded = excluded;
            }

            ShapeNode getRequired() {
                return required;
            }

            void setRequired(ShapeNode required) {
                this.required = required;
            }

            ShapeNode getExcluded() {
                return excluded;
            }

            void setExcluded(ShapeNode excluded) {
                this.excluded = excluded;
            }
        }

        non-sealed class AndShape implements ShapeNode {

            private ShapeNode left;
            private ShapeNode right;

            AndShape(ShapeNode left, ShapeNode right) {
                this.left = left;
                this.right = right;
            }

            ShapeNode getLeft() {
                return left;
            }

            void setLeft(ShapeNode left) {
                this.left = left;
            }

            ShapeNode getRight() {
                return right;
            }

            void setRight(ShapeNode right) {
                this.right = right;
            }
        }
    }

    String AND_NOT_MARKER = " AND NOT (";
    /**
     * A plain (non-negating) conjunction marker — deliberately distinct text from
     * {@link #AND_NOT_MARKER} ({@code " NOT "} sits between "AND" and the paren there), so checking
     * {@link #AND_NOT_MARKER} first, as {@link #parseShape} does, can never misfire on this one, and
     * vice versa: this marker's own {@code findTopLevel} scan never matches inside an occurrence of
     * {@link #AND_NOT_MARKER}.
     */
    String AND_MARKER = " AND (";
    Pattern PROXIMITY_KEYWORD = Pattern.compile(" (" + OPERATOR_NEAR + "|" + OPERATOR_FOLLOWEDBY + ")\\{(\\d+)\\} ");

    /**
     * Checks {@link #AND_NOT_MARKER} first, then {@link #AND_MARKER} — a term combining both in one
     * {@code resolvedPatterns} string (e.g. a plain AND together with an AND NOT) has not been
     * observed against real data and is not handled here; the whole string falls through to
     * {@link #AND_NOT_MARKER}'s branch in that case, same as before this method learned about
     * {@link #AND_MARKER} at all.
     */
    static ShapeNode parseShape(String feature, String termId, String text) {
        int andNotAt = findTopLevel(text, AND_NOT_MARKER);
        if (andNotAt >= 0) {
            String requiredText = text.substring(0, andNotAt);
            int openParenAt = andNotAt + AND_NOT_MARKER.length() - 1;
            int closeParenAt = matchingCloseParen(feature, termId, text, openParenAt);
            String excludedText = text.substring(openParenAt + 1, closeParenAt);
            return new ShapeNode.AndNotShape(parseChainShape(requiredText), parseChainShape(excludedText));
        }
        int andAt = findTopLevel(text, AND_MARKER);
        if (andAt >= 0) {
            String leftText = text.substring(0, andAt);
            int openParenAt = andAt + AND_MARKER.length() - 1;
            int closeParenAt = matchingCloseParen(feature, termId, text, openParenAt);
            String rightText = text.substring(openParenAt + 1, closeParenAt);
            return new ShapeNode.AndShape(parseChainShape(leftText), parseChainShape(rightText));
        }
        return parseChainShape(text);
    }

    static ShapeNode.ChainShape parseChainShape(String text) {
        List<String> operators = new ArrayList<>();
        List<Integer> distances = new ArrayList<>();

        int depth = 0;
        int leafCount = 0;
        int charIndex = 0;
        while (charIndex < text.length()) {
            char currentChar = text.charAt(charIndex);
            if (currentChar == '(') {
                depth++;
            } else if (currentChar == ')') {
                depth--;
            }
            if (depth == 0) {
                Matcher proximityMatcher = PROXIMITY_KEYWORD.matcher(text);
                proximityMatcher.region(charIndex, text.length());
                if (proximityMatcher.lookingAt()) {
                    leafCount++;
                    operators.add(proximityMatcher.group(1));
                    distances.add(Integer.parseInt(proximityMatcher.group(2)));
                    charIndex = proximityMatcher.end();
                    continue;
                }
            }
            charIndex++;
        }
        leafCount++; // the final segment after the last operator (or the only segment, if none)
        return new ShapeNode.ChainShape(leafCount, operators, distances);
    }

    /**
     * First TOP-level (paren-depth 0) occurrence of {@code marker}, or -1.
     */
    static int findTopLevel(String text, String marker) {
        int depth = 0;
        for (int charIndex = 0; charIndex <= text.length() - marker.length(); charIndex++) {
            char currentChar = text.charAt(charIndex);
            if (currentChar == '(') {
                depth++;
            } else if (currentChar == ')') {
                depth--;
            }
            if (depth == 0 && text.startsWith(marker, charIndex)) {
                return charIndex;
            }
        }
        return -1;
    }

    static int matchingCloseParen(String feature, String termId, String text, int openParenAt) {
        int depth = 0;
        for (int charIndex = openParenAt; charIndex < text.length(); charIndex++) {
            if (text.charAt(charIndex) == '(') {
                depth++;
            } else if (text.charAt(charIndex) == ')') {
                depth--;
                if (depth == 0) {
                    return charIndex;
                }
            }
        }
        throw new TermExpressionMetadata.TermMetadataParseException(
                "Term '" + termId + "' in feature '" + feature + "' has unbalanced parentheses in "
                        + "resolvedPatterns: " + text);
    }

    // ── Zipping the discovered shape against regexPattern's leaves ──────────

    static ResolvedPatternTree zip(String feature, String termId, ShapeNode shape, Iterator<String> cursor) {
        if (shape instanceof ShapeNode.AndNotShape andNot) {
            return new AndNot(
                    zip(feature, termId, andNot.getRequired(), cursor),
                    zip(feature, termId, andNot.getExcluded(), cursor));
        }
        if (shape instanceof ShapeNode.AndShape and) {
            return new And(
                    zip(feature, termId, and.getLeft(), cursor),
                    zip(feature, termId, and.getRight(), cursor));
        }
        ShapeNode.ChainShape chainShape = (ShapeNode.ChainShape) shape;
        List<Pattern> leaves = new ArrayList<>(chainShape.getLeafCount());
        for (int leafPosition = 0; leafPosition < chainShape.getLeafCount(); leafPosition++) {
            if (!cursor.hasNext()) {
                throw new TermExpressionMetadata.TermMetadataParseException(
                        "Term '" + termId + "' in feature '" + feature + "': resolvedPatterns' shape implies more "
                                + "leaves than regexPattern/translatedPattern provides.");
            }
            leaves.add(Pattern.compile(cursor.next(), JAVA_LEAF_FLAGS));
        }
        return new Chain(leaves, chainShape.getOperators(), chainShape.getDistances());
    }

    /**
     * Total leaf count across a whole tree, recursively — used to validate {@code patternMapping}'s
     * id count against a tree that may be a plain {@link Chain}, an {@link AndNot}, or (now) an
     * {@link And}, without the caller needing to know which.
     */
    static int countLeaves(ResolvedPatternTree tree) {
        if (tree instanceof Chain chain) {
            return chain.getLeaves().size();
        }
        if (tree instanceof AndNot andNot) {
            return countLeaves(andNot.getRequired()) + countLeaves(andNot.getExcluded());
        }
        if (tree instanceof And and) {
            return countLeaves(and.getLeft()) + countLeaves(and.getRight());
        }
        throw new IllegalStateException("Unknown ResolvedPatternTree implementation: " + tree.getClass());
    }
}
