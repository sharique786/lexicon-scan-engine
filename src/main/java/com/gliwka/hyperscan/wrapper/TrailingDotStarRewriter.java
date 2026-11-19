package com.gliwka.hyperscan.wrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Makes terms ending in {@code .*} (e.g. {@code This email may contain confidential.*}) scannable.
 *
 * <p>Under {@code SOM_LEFTMOST} a trailing {@code .*} makes Hyperscan (a) report a match at every end
 * offset (memory blow-up) and (b) attribute the whole tail of the text to the FIRST occurrence's start, so
 * a second/third occurrence of the same disclaimer is never reported separately. The fix is to compile the
 * pattern WITHOUT the trailing {@code .*}: Hyperscan then reports exactly one match per occurrence of the
 * prefix. {@link CollapsingScanner} extends each such match to the end of its sentence (see
 * {@link CollapsingScanner#MAX_TAIL_CHARS}) — it knows which ids to extend from {@link #tailIds}.
 *
 * <p>Only plain (non-{@code COMBINATION}, non-{@code QUIET}) expressions are rewritten; their pattern text
 * as seen on a {@link Match} no longer carries the {@code .*}.
 */
public final class TrailingDotStarRewriter {

    private static final Map<Database, Set<Integer>> TAIL_IDS =
            Collections.synchronizedMap(new WeakHashMap<>());

    private TrailingDotStarRewriter() {
    }

    /**
     * Returns {@code original} unchanged if no expression ends with {@code .*}; otherwise closes it and
     * returns a recompiled database (same ids and flags) whose such expressions have the suffix removed.
     */
    public static Database apply(Database original) {
        List<Expression> all = new ArrayList<>();
        Set<Integer> rewrittenIds = new HashSet<>();
        for (int id = 0; ; id++) {
            Expression e;
            try {
                e = original.getExpression(id);
            } catch (ArrayIndexOutOfBoundsException end) {
                break;
            }
            if (e == null) {
                continue;
            }
            String stripped = stripTrailingDotStar(e);
            if (stripped == null) {
                all.add(e);
            } else {
                all.add(new Expression(stripped, e.getFlags(), e.getId()));
                rewrittenIds.add(e.getId());
            }
        }
        if (rewrittenIds.isEmpty()) {
            return original;
        }
        Database rewritten;
        try {
            rewritten = Database.compile(all);
        } catch (CompileErrorException e) {
            throw new IllegalStateException("Recompiling without trailing .* failed: " + e.getMessage(), e);
        }
        original.close();
        TAIL_IDS.put(rewritten, rewrittenIds);
        return rewritten;
    }

    /** Ids whose trailing {@code .*} was removed from {@code db}; empty if none. */
    public static Set<Integer> tailIds(Database db) {
        Set<Integer> ids = TAIL_IDS.get(db);
        return ids == null ? Set.of() : ids;
    }

    private static String stripTrailingDotStar(Expression e) {
        Set<ExpressionFlag> flags = e.getFlags();
        if (e.getId() == null || flags.contains(ExpressionFlag.COMBINATION) || flags.contains(ExpressionFlag.QUIET)) {
            return null;
        }
        String p = e.getExpression();
        if (!p.endsWith(".*") || p.length() == 2) {
            return null;
        }
        // an escaped dot ("\.*") is a literal-dot repetition, not "any char"
        int backslashes = 0;
        for (int i = p.length() - 3; i >= 0 && p.charAt(i) == '\\'; i--) {
            backslashes++;
        }
        if (backslashes % 2 == 1) {
            return null;
        }
        return p.substring(0, p.length() - 2);
    }
}
