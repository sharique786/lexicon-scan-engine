package com.gliwka.hyperscan.wrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * Keyed by database IDENTITY, not equality: {@code Database.equals} compares expression content, so two
     * distinct databases built from the same bundle (e.g. two tasks on one executor) are "equal", and a
     * {@code WeakHashMap} would drop one's entry when the other is collected. Weak so a closed database's
     * entry goes away with it.
     */
    private static final Map<IdentityRef, Set<Integer>> TAIL_IDS = new ConcurrentHashMap<>();
    private static final ReferenceQueue<Database> STALE = new ReferenceQueue<>();

    private static final class IdentityRef extends WeakReference<Database> {
        private final int hash;

        IdentityRef(Database db, ReferenceQueue<Database> queue) {
            super(db, queue);
            this.hash = System.identityHashCode(db);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IdentityRef other)) {
                return false;
            }
            Database mine = get();
            return mine != null && mine == other.get();
        }
    }

    private static void expungeStale() {
        for (Reference<? extends Database> ref; (ref = STALE.poll()) != null; ) {
            TAIL_IDS.remove(ref);
        }
    }

    private TrailingDotStarRewriter() {
    }

    /**
     * Returns {@code original} unchanged if no expression ends with {@code .*}; otherwise closes it and
     * returns a recompiled database (same ids and flags) whose such expressions have the suffix removed.
     *
     * <p>Reads the expressions back through the public {@link Database#save}, which costs a native
     * serialisation of the database. Prefer {@link #apply(Database, byte[])} when the original {@code .hdb}
     * bytes are at hand.
     */
    public static Database apply(Database original) {
        try {
            ByteArrayOutputStream expressionsOut = new ByteArrayOutputStream();
            original.save(expressionsOut, OutputStream.nullOutputStream());
            return apply(original, readExpressions(new ByteArrayInputStream(expressionsOut.toByteArray())));
        } catch (IOException e) {
            throw new IllegalStateException("Could not read back the database's expressions: " + e.getMessage(), e);
        }
    }

    /**
     * Same as {@link #apply(Database)}, but takes the expressions from the {@code hdbBytes} the database was
     * loaded from (they start with the expression header written by {@link Database#save}), so nothing is
     * re-serialised natively.
     */
    public static Database apply(Database original, byte[] hdbBytes) {
        try {
            return apply(original, readExpressions(new ByteArrayInputStream(hdbBytes)));
        } catch (IOException e) {
            throw new IllegalStateException("Could not read the expression header of the .hdb: " + e.getMessage(), e);
        }
    }

    private static Database apply(Database original, List<Expression> expressions) {
        List<Expression> all = new ArrayList<>(expressions.size());
        Set<Integer> rewrittenIds = new HashSet<>();
        for (Expression e : expressions) {
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
        expungeStale();
        TAIL_IDS.put(new IdentityRef(rewritten, STALE), rewrittenIds);
        return rewritten;
    }

    /**
     * Parses the expression header {@link Database#save} writes first: count, then per expression
     * {@code id (-1 = none), UTF pattern, flag count, flag bitmasks}. The format is identical in hyperscan-java
     * 5.4.0-2.0.0 and 5.4.11-3.1.0, and - unlike {@code Database.getExpression(int)}/{@code getExpressionCount()} -
     * it needs no package-private member that exists in only one of them.
     */
    static List<Expression> readExpressions(InputStream in) throws IOException {
        DataInputStream data = new DataInputStream(in);
        int count = data.readInt();
        Map<Integer, ExpressionFlag> byBits = new HashMap<>();
        for (ExpressionFlag flag : ExpressionFlag.values()) {
            byBits.put(flag.getBits(), flag);
        }
        List<Expression> expressions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int id = data.readInt();
            String pattern = data.readUTF();
            int flagCount = data.readInt();
            EnumSet<ExpressionFlag> flags = EnumSet.noneOf(ExpressionFlag.class);
            for (int j = 0; j < flagCount; j++) {
                ExpressionFlag flag = byBits.get(data.readInt());
                if (flag != null) {
                    flags.add(flag);
                }
            }
            expressions.add(new Expression(pattern, flags, id == -1 ? null : id));
        }
        return expressions;
    }

    /** Ids whose trailing {@code .*} was removed from {@code db}; empty if none. */
    public static Set<Integer> tailIds(Database db) {
        Set<Integer> ids = TAIL_IDS.get(new IdentityRef(db, null));
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
