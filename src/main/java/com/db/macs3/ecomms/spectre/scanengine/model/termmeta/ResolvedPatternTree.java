package com.db.macs3.ecomms.spectre.scanengine.model.termmeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The evaluable form of one term's {@code resolvedPatterns} string — the
 * Lexicon Compile Service's new, decomposed representation of a NEAR/
 * FOLLOWEDBY proximity chain and/or an AND NOT condition, kept as a
 * left-to-right sequence of {@code regexPattern} leaves joined by the
 * operator text {@code resolvedPatterns} carries. See
 * {@code ResolvedPatternAreaEvaluator} for how a tree is actually evaluated
 * against real message text, and {@code TermExpressionMetadata} class
 * Javadoc for why this exists at all (decomposed leaves are compiled QUIET
 * in the {@code .hdb} — Hyperscan itself can never report their individual
 * positions, so the real proximity/AND-NOT condition must be re-verified in
 * Java against the leaves' own regex text).
 *
 * <h2>Deliberate improvement over the {@code ResolvedPatternMatcher} reference class</h2>
 * <p>The attached reference implementation slices each leaf's own regex text
 * directly out of the {@code resolvedPatterns} string via top-level text
 * scanning — safe only so long as a leaf's regex text never happens to
 * contain the literal substring {@code " NEAR{5} "} etc. (an assumption its
 * own Javadoc admits, not a hardened guarantee). This project's JSON already
 * provides {@code regexPattern} as a clean, structured list of exactly the
 * leaf texts, in the same left-to-right order {@code resolvedPatterns}
 * renders them in — so {@link #build} instead uses the reference's
 * paren-depth-aware scanning ONLY to discover the tree's SHAPE (leaf count
 * per chain segment, operator+distance sequence, the AND NOT split point),
 * then zips that shape against {@code regexPattern} positionally. No leaf
 * regex text is ever sliced out of {@code resolvedPatterns} itself, and a
 * leaf-count disagreement between the two fields becomes a structural parse
 * error (the zip cursor running out, or having leftovers) rather than a
 * silent mismatch.
 *
 * <h2>Not {@code Serializable} — deliberately</h2>
 * <p>{@link Pattern} does not implement {@link java.io.Serializable}, so
 * neither can this class. This is safe today only because
 * {@code TermMetadataLoader} (like {@code HyperscanDatabaseLoader}) is
 * constructed INSIDE {@code PartitionProcessor}'s {@code mapPartitions}
 * closure body — every {@code TermExpressionMetadata}/{@code TermEntry}/
 * {@code ResolvedPatternTree} is built fresh, executor-local, per partition,
 * and never serialized across the wire. If a future refactor ever moves
 * that construction to the driver and ships the loaded object via closure
 * capture, this assumption breaks with a {@code NotSerializableException} —
 * do not "fix" that by making this Serializable; fix the refactor instead.
 */
public sealed interface ResolvedPatternTree {

    /**
     * A (possibly length-1, i.e. no proximity operator at all) sequence of
     * leaves connected by NEAR/FOLLOWEDBY.
     *
     * @param leaves    one compiled, case-insensitive pattern per leaf, in left-to-right term order
     * @param operators {@code "NEAR"} or {@code "FOLLOWEDBY"} between consecutive leaves —
     *                  {@code operators.size() == leaves.size() - 1}
     * @param distances the raw distance for each operator — same size as {@code operators}
     */
    record Chain(List<Pattern> leaves, List<String> operators, List<Integer> distances) implements ResolvedPatternTree {
    }

    /**
     * @param required the required side — always a plain {@code Chain}, never a further {@code AndNot}
     *                 (AND NOT is never nested, per the Compile Service's own documented contract)
     * @param excluded the excluded side — always a plain {@code Chain}
     */
    record AndNot(ResolvedPatternTree required, ResolvedPatternTree excluded) implements ResolvedPatternTree {
    }

    int JAVA_LEAF_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS;

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
        record ChainShape(int leafCount, List<String> operators, List<Integer> distances) implements ShapeNode {
        }

        record AndNotShape(ShapeNode required, ShapeNode excluded) implements ShapeNode {
        }
    }

    String AND_NOT_MARKER = " AND NOT (";
    Pattern PROXIMITY_KEYWORD = Pattern.compile(" (NEAR|FOLLOWEDBY)\\{(\\d+)\\} ");

    static ShapeNode parseShape(String feature, String termId, String text) {
        int markerAt = findTopLevel(text, AND_NOT_MARKER);
        if (markerAt < 0) {
            return parseChainShape(text);
        }
        String requiredText = text.substring(0, markerAt);
        int openParenAt = markerAt + AND_NOT_MARKER.length() - 1;
        int closeParenAt = matchingCloseParen(feature, termId, text, openParenAt);
        String excludedText = text.substring(openParenAt + 1, closeParenAt);
        return new ShapeNode.AndNotShape(parseChainShape(requiredText), parseChainShape(excludedText));
    }

    static ShapeNode.ChainShape parseChainShape(String text) {
        List<String> operators = new ArrayList<>();
        List<Integer> distances = new ArrayList<>();

        int depth = 0;
        int leafCount = 0;
        int segmentStart = 0;
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            if (depth == 0) {
                Matcher m = PROXIMITY_KEYWORD.matcher(text);
                m.region(i, text.length());
                if (m.lookingAt()) {
                    leafCount++;
                    operators.add(m.group(1));
                    distances.add(Integer.parseInt(m.group(2)));
                    i = m.end();
                    segmentStart = i;
                    continue;
                }
            }
            i++;
        }
        leafCount++; // the final segment after the last operator (or the only segment, if none)
        return new ShapeNode.ChainShape(leafCount, operators, distances);
    }

    /** First TOP-level (paren-depth 0) occurrence of {@code marker}, or -1. */
    static int findTopLevel(String text, String marker) {
        int depth = 0;
        for (int i = 0; i <= text.length() - marker.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            if (depth == 0 && text.startsWith(marker, i)) {
                return i;
            }
        }
        return -1;
    }

    static int matchingCloseParen(String feature, String termId, String text, int openParenAt) {
        int depth = 0;
        for (int i = openParenAt; i < text.length(); i++) {
            if (text.charAt(i) == '(') {
                depth++;
            } else if (text.charAt(i) == ')') {
                depth--;
                if (depth == 0) {
                    return i;
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
                    zip(feature, termId, andNot.required(), cursor),
                    zip(feature, termId, andNot.excluded(), cursor));
        }
        ShapeNode.ChainShape chainShape = (ShapeNode.ChainShape) shape;
        List<Pattern> leaves = new ArrayList<>(chainShape.leafCount());
        for (int i = 0; i < chainShape.leafCount(); i++) {
            if (!cursor.hasNext()) {
                throw new TermExpressionMetadata.TermMetadataParseException(
                        "Term '" + termId + "' in feature '" + feature + "': resolvedPatterns' shape implies more "
                        + "leaves than regexPattern/translatedPattern provides.");
            }
            leaves.add(Pattern.compile(cursor.next(), JAVA_LEAF_FLAGS));
        }
        return new Chain(leaves, chainShape.operators(), chainShape.distances());
    }
}
