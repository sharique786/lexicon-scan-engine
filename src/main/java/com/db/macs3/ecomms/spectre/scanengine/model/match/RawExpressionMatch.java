package com.db.macs3.ecomms.spectre.scanengine.model.match;

import java.io.Serializable;
import java.util.List;

/**
 * Every match found for ONE raw Hyperscan expression id within a single
 * area of a message — NOT yet resolved to a term identity or evaluated for
 * an AND NOT boolean condition. {@link com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanScanService#scan}
 * returns these; {@code FeatureScanOrchestrator} merges them across every
 * area a feature's scope covers, then resolves and evaluates them into
 * final {@link TermMatchResult}s using {@code TermExpressionMetadata}.
 *
 * @param expressionId        the raw Hyperscan expression id, as reported on a {@code Match} —
 *                              may belong to a simple/decomposed term's own reportable id, or to
 *                              ONE of an AND NOT term's required/excluded ids; the caller
 *                              (with {@code TermExpressionMetadata}) determines which
 * @param matchedPatternText   the pattern text read directly off the {@code Match}'s own
 *                              {@code Expression} — reliable for a plain or COMBINATION
 *                              expression's own text; not meaningful as a display value for an
 *                              AND NOT term's excluded-side id (see {@code FeatureScanOrchestrator})
 * @param matches               every occurrence found for this expression id within this one area —
 *                              non-empty
 */
public record RawExpressionMatch(
        int expressionId,
        String matchedPatternText,
        List<AreaMatch> matches
) implements Serializable {
    public RawExpressionMatch {
        if (matches == null || matches.isEmpty()) {
            throw new IllegalArgumentException("RawExpressionMatch requires at least one match for expressionId=" + expressionId);
        }
    }
}
