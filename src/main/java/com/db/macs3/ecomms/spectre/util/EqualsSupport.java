package com.db.macs3.ecomms.spectre.util;

import java.util.Objects;

/**
 * Shared helper for data classes with many fields, whose {@code equals()}
 * would otherwise chain a long {@code Objects.equals(...) && ...} expression —
 * every {@code &&} is its own decision point for cyclomatic-complexity
 * purposes, so a wide row class's {@code equals()} easily exceeds a CC
 * threshold despite being a single, uniform comparison. Delegating the
 * pairwise comparison to a loop here keeps the CC of every calling
 * {@code equals()} constant (2: the {@code this == obj} and
 * {@code getClass()} checks) regardless of field count.
 */
public final class EqualsSupport {

    private EqualsSupport() {
    }

    /**
     * @param fieldAndOtherFieldPairs this object's fields interleaved with the other object's
     *                                corresponding fields, in pairs: {@code a1, b1, a2, b2, ...}
     * @return true iff every pair is equal per {@link Objects#equals(Object, Object)}
     */
    public static boolean fieldsEqual(Object... fieldAndOtherFieldPairs) {
        for (int i = 0; i < fieldAndOtherFieldPairs.length; i += 2) {
            if (!Objects.equals(fieldAndOtherFieldPairs[i], fieldAndOtherFieldPairs[i + 1])) {
                return false;
            }
        }
        return true;
    }
}
