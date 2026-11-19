package com.gliwka.hyperscan.wrapper;

import com.gliwka.hyperscan.jni.hs_database_t;
import com.gliwka.hyperscan.jni.hs_scratch_t;
import com.gliwka.hyperscan.jni.match_event_handler;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Pointer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;

import static com.gliwka.hyperscan.jni.hyperscan.hs_alloc_scratch;
import static com.gliwka.hyperscan.jni.hyperscan.hs_free_scratch;
import static com.gliwka.hyperscan.jni.hyperscan.hs_scan;

/**
 * Drop-in replacement for the wrapper's {@link Scanner} that is safe for patterns ending in
 * {@code .*} (e.g. {@code This email may contain confidential.*}) under {@code SOM_LEFTMOST}.
 *
 * <h2>Why the stock {@code Scanner} runs out of memory</h2>
 * Hyperscan reports a match at EVERY end offset a pattern can end at. For {@code foo.*} that is every
 * offset from the end of {@code foo} to the end of the text, each with the same start. The stock
 * {@code Scanner.scan} stores every callback tuple and then builds a {@code substring(start, end)} for
 * each: for a disclaimer at the top of a 1M-char body that is ~1M matches AND ~n^2/2 characters of
 * substrings. Several disclaimers (or several {@code .*} terms) multiply it.
 *
 * <h2>What this class does instead</h2>
 * <ul>
 *   <li>The native callback keeps only the LONGEST end per {@code (expressionId, start)} — O(distinct
 *       starts) memory instead of O(match callbacks). {@code SOM_LEFTMOST} is untouched, so start
 *       positions are exactly what Hyperscan reports.</li>
 *   <li>Substrings are built once per surviving match, not per callback.</li>
 *   <li>Because {@code .*} is greedy and Hyperscan reports leftmost starts, for a given expression the
 *       first disclaimer's start owns the whole tail of the text; later disclaimers of the SAME
 *       expression collapse into that one match (Hyperscan itself never reports a distinct later start
 *       for them). Different expressions each keep their own match.</li>
 * </ul>
 * Trade-off: for non-{@code .*} patterns with several possible ends from one start (e.g. {@code a+}),
 * only the longest is returned rather than every prefix — those extra prefix matches were never
 * meaningful hits.
 *
 * <p>Lives in the wrapper's package only because {@link Database#getDatabase()},
 * {@link Database#getExpression(int)} are package-private. Uses only members present in BOTH
 * hyperscan-java 5.4.0-2.0.0 and 5.4.11-3.1.0. Same lifecycle contract as
 * {@link Scanner}: one instance per thread, {@link #close()} when done, at most 256 live instances.
 */
public class CollapsingScanner implements Closeable {

    private static int count = 0;

    private static class NativeScratch extends hs_scratch_t {
        private NativeScratch() {
            super();
            this.deallocator(() -> hs_free_scratch(this));
        }
    }

    private NativeScratch scratch = new NativeScratch();

    /** key = (expressionId << 32 | startByteOffset) -> {id, fromByte, toByte (max)}; insertion-ordered. */
    private final Map<Long, long[]> collapsed = new LinkedHashMap<>();

    private final match_event_handler handler = new match_event_handler() {
        @Override
        public int call(int id, long from, long to, int flags, Pointer context) {
            long key = ((long) id << 32) | (from & 0xFFFFFFFFL);
            long[] entry = collapsed.get(key);
            if (entry == null) {
                collapsed.put(key, new long[]{id, from, to});
            } else if (to > entry[2]) {
                entry[2] = to;
            }
            return 0; // keep scanning
        }
    };

    public CollapsingScanner() {
        synchronized (CollapsingScanner.class) { // several Spark tasks share one executor JVM
            if (count >= 256) {
                throw new RuntimeException(
                        "There can only be 256 non-closed Scanner instances. Create them once per thread!");
            }
            count++;
        }
    }

    /** Must be called at least once per database before {@link #scan}; cheap and idempotent. */
    public void allocScratch(Database db) {
        if (scratch == null) {
            throw new IllegalStateException("Scratch space has already been deallocated");
        }
        if (db == lastScratchDb) {
            return; // scratch already covers this exact database
        }
        int hsError = hs_alloc_scratch(db.getDatabase(), scratch);
        if (hsError != 0) {
            throw HyperscanException.hsErrorToException(hsError);
        }
        lastScratchDb = db;
    }

    /** The last database {@link #allocScratch} succeeded for — lets repeat calls for it be skipped. */
    private Database lastScratchDb;

    public List<Match> scan(Database db, String input) {
        return scan(db, new PreparedText(input));
    }

    /** Same as {@link #scan(Database, String)} but reuses already-encoded text — see {@link PreparedText}. */
    public List<Match> scan(Database db, PreparedText prepared) {
        if (scratch == null) {
            throw new IllegalStateException("Scratch space has already been deallocated");
        }
        String input = prepared.text();
        hs_database_t database = db.getDatabase();
        byte[] bytes = prepared.bytes();
        BytePointer bytePointer = new BytePointer(ByteBuffer.wrap(bytes));

        collapsed.clear();
        try {
            int hsError = hs_scan(database, bytePointer, bytes.length, 0, scratch, handler, null);
            if (hsError != 0) {
                throw HyperscanException.hsErrorToException(hsError);
            }
            if (collapsed.isEmpty()) {
                return List.of();
            }

            IntUnaryOperator toCharIndex;
            if (prepared.isAscii()) {
                toCharIndex = p -> p; // pure ASCII
            } else {
                int[] map = prepared.positionMap();
                toCharIndex = p -> map[p];
            }

            java.util.Set<Integer> tailIds = TrailingDotStarRewriter.tailIds(db);
            // per tail-id: sorted start offsets, so a tail never runs into the next occurrence
            Map<Integer, int[]> startsById = new java.util.HashMap<>();
            if (!tailIds.isEmpty()) {
                Map<Integer, List<Integer>> tmp = new java.util.HashMap<>();
                for (long[] t : collapsed.values()) {
                    if (tailIds.contains((int) t[0])) {
                        tmp.computeIfAbsent((int) t[0], k -> new ArrayList<>())
                                .add(toCharIndex.applyAsInt((int) t[1]));
                    }
                }
                tmp.forEach((k, v) -> startsById.put(k, v.stream().mapToInt(Integer::intValue).sorted().toArray()));
            }

            List<Match> matches = new ArrayList<>(collapsed.size());
            for (long[] t : collapsed.values()) {
                int id = (int) t[0];
                long to = t[2] < 1 ? 1 : t[2];
                Expression expression = db.getExpression(id);
                int startIndex = toCharIndex.applyAsInt((int) t[1]);
                int endIndex = toCharIndex.applyAsInt((int) to - 1);
                if (tailIds.contains(id)) {
                    endIndex = extendToSentenceEnd(input, startIndex, endIndex, startsById.get(id));
                }
                String matched = expression.getFlags().contains(ExpressionFlag.SOM_LEFTMOST)
                        ? input.substring(startIndex, endIndex + 1) : "";
                matches.add(bytes.length > 0
                        ? new Match(startIndex, endIndex, matched, expression)
                        : new Match(0, 0, matched, expression));
            }
            return matches;
        } finally {
            collapsed.clear(); // never retain a large map between scans
        }
    }

    /** A rewritten trailing {@code .*} never extends a match by more than this many characters. */
    public static final int MAX_TAIL_CHARS = 500;

    /**
     * Emulates the removed {@code .*} as "to the end of the sentence": through the first {@code . ! ?} or
     * line break after the prefix (inclusive), else to end of text — never beyond {@link #MAX_TAIL_CHARS},
     * and never into the next occurrence of the same expression.
     *
     * @return the new INCLUSIVE end index
     */
    private static int extendToSentenceEnd(String text, int start, int prefixEndInclusive, int[] sortedStarts) {
        int limit = Math.min(text.length(), prefixEndInclusive + 1 + MAX_TAIL_CHARS); // exclusive
        if (sortedStarts != null) {
            for (int s : sortedStarts) {
                if (s > prefixEndInclusive) {
                    limit = Math.min(limit, s);
                    break;
                }
            }
        }
        int end = prefixEndInclusive;
        for (int i = prefixEndInclusive + 1; i < limit; i++) {
            char c = text.charAt(i);
            if (c == '\n' || c == '\r') {
                break; // line break is excluded, exactly like ".*"
            }
            end = i;
            if (c == '.' || c == '!' || c == '?') {
                break;
            }
        }
        return end;
    }

    @Override
    public void close() {
        if (scratch == null) {
            return;
        }
        scratch.close();
        handler.close();
        synchronized (CollapsingScanner.class) {
            count--;
        }
        scratch = null;
        lastScratchDb = null;
    }
}
