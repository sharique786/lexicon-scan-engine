package com.gliwka.hyperscan.wrapper;

import com.gliwka.hyperscan.jni.hs_database_t;
import com.gliwka.hyperscan.jni.hs_scratch_t;
import com.gliwka.hyperscan.jni.match_event_handler;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Pointer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *       positions are exactly what Hyperscan reports. The bookkeeping is a primitive open-addressing table
 *       ({@link MatchCollector}), so a callback allocates nothing (no boxed key per match).</li>
 *   <li>A {@link PreparedText}'s native copy is reused across scans: scanning one area text against many
 *       feature databases copies it into native memory once, not once per database.</li>
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

    private static final class NativeScratch extends hs_scratch_t {
        private NativeScratch() {
            super();
        }

        /** Registers the deallocator after construction (calling the overridable method from the constructor is unsafe). */
        static NativeScratch create() {
            NativeScratch created = new NativeScratch();
            created.deallocator(() -> hs_free_scratch(created));
            return created;
        }
    }

    /** Thrown when more than {@link #MAX_LIVE_SCANNERS} scanners are open at once. */
    public static final class TooManyScannersException extends IllegalStateException {
        public TooManyScannersException(String message) {
            super(message);
        }
    }

    private static final int MAX_LIVE_SCANNERS = 256;

    // Both native; allocated in the constructor only AFTER the live-instance check, so a rejected construction
    // leaks nothing (as field initialisers they ran first and were never freed when the check threw).
    private NativeScratch scratch;

    /** (expressionId, startByteOffset) -> longest end byte offset, in first-seen order. */
    private final MatchCollector collapsed = new MatchCollector();

    private final match_event_handler handler;

    /** Upper bound on how far a rewritten trailing {@code .*} extends a match — see {@link #extendToSentenceEnd}. */
    private final int maxTailChars;

    /**
     * First Throwable raised inside the native match callback during the current scan, or null. A Java exception
     * must never escape a JavaCPP callback: JavaCPP rethrows it as a C++ exception through Hyperscan's C frames,
     * which cannot unwind them, and the whole JVM dies. The callback records it, stops the scan, and
     * {@link #runNativeScan} rethrows it on the Java side.
     */
    private Throwable callbackFailure;

    private match_event_handler newHandler() {
        return new match_event_handler() {
            @Override
            public int call(int id, long from, long to, int flags, Pointer context) {
                try {
                    collapsed.add(id, from, to);
                    return 0; // keep scanning
                } catch (Throwable t) { // NOSONAR must not cross the native boundary, see callbackFailure
                    if (callbackFailure == null) {
                        callbackFailure = t;
                    }
                    return 1; // non-zero stops the scan (HS_SCAN_TERMINATED)
                }
            }
        };
    }

    /** A scanner whose trailing-{@code .*} tails are capped at {@link #DEFAULT_MAX_TAIL_CHARS}. */
    public CollapsingScanner() {
        this(DEFAULT_MAX_TAIL_CHARS);
    }

    /**
     * @param maxTailChars upper bound, in characters, on how far a rewritten trailing {@code .*} extends a match
     *                     (see {@link #extendToSentenceEnd}); must be &gt;= 0
     */
    public CollapsingScanner(int maxTailChars) {
        if (maxTailChars < 0) {
            throw new IllegalArgumentException("maxTailChars must be >= 0, was " + maxTailChars);
        }
        this.maxTailChars = maxTailChars;
        synchronized (CollapsingScanner.class) { // several Spark tasks share one executor JVM
            if (count >= MAX_LIVE_SCANNERS) {
                throw new TooManyScannersException(
                        "There can only be 256 non-closed Scanner instances. Create them once per thread!");
            }
            count++;
        }
        boolean constructed = false;
        try {
            this.scratch = NativeScratch.create();
            this.handler = newHandler();
            constructed = true;
        } finally {
            if (!constructed) {
                if (scratch != null) {
                    scratch.close();
                    scratch = null;
                }
                synchronized (CollapsingScanner.class) {
                    count--;
                }
            }
        }
    }

    /** Number of scanners currently open in this JVM (diagnostics/tests). */
    public static int liveCount() {
        synchronized (CollapsingScanner.class) {
            return count;
        }
    }

    /** Must be called at least once per database before {@link #scan}; cheap and idempotent. */
    public void allocScratch(Database db) {
        if (scratch == null) {
            throw new IllegalStateException("Scratch space has already been deallocated");
        }
        if (db == lastScratchDb) { // NOSONAR identity on purpose: scratch belongs to this exact native database
            return; // scratch already covers this exact database
        }
        int hsError = hs_alloc_scratch(db.getDatabase(), scratch);
        if (hsError != 0) {
            throw HyperscanException.hsErrorToException(hsError);
        }
        lastScratchDb = db;
    }

    /**
     * Drops this scanner's references to the databases it last scanned (scratch and tail-id shortcuts), keeping its
     * native scratch and callback. Called when a pooled scanner is handed back, so it never pins a database that the
     * finished task released; the next {@link #allocScratch} simply re-checks the scratch (cheap).
     */
    public void forgetDatabases() {
        lastScratchDb = null;
        lastTailDb = null;
        lastTailIds = Set.of();
    }

    /** The last database {@link #allocScratch} succeeded for — lets repeat calls for it be skipped. */
    private Database lastScratchDb;

    /** One-off scan: the text's native copy is freed as soon as the scan is done. */
    public List<Match> scan(Database db, String input) {
        try (PreparedText prepared = new PreparedText(input)) {
            return scan(db, prepared);
        }
    }

    /**
     * Same as {@link #scan(Database, String)} but reuses already-encoded text — and its native copy — across
     * scans; see {@link PreparedText}. The caller owns {@code prepared} and must {@link PreparedText#close()} it.
     */
    public List<Match> scan(Database db, PreparedText prepared) {
        if (scratch == null) {
            throw new IllegalStateException("Scratch space has already been deallocated");
        }
        collapsed.clear();
        try {
            runNativeScan(db, prepared);
            if (collapsed.isEmpty()) {
                return List.of();
            }
            return buildMatches(db, prepared);
        } finally {
            collapsed.clear(); // never retain a large table between scans
        }
    }

    private void runNativeScan(Database db, PreparedText prepared) {
        hs_database_t database = db.getDatabase();
        BytePointer bytePointer = prepared.nativeBytes();
        callbackFailure = null;
        int hsError = hs_scan(database, bytePointer, prepared.byteLength(), 0, scratch, handler, null);
        Throwable failure = callbackFailure;
        callbackFailure = null;
        if (failure != null) {
            throw new IllegalStateException("Hyperscan match callback failed: " + failure, failure);
        }
        if (hsError != 0) {
            throw HyperscanException.hsErrorToException(hsError);
        }
    }

    private List<Match> buildMatches(Database db, PreparedText prepared) {
        String input = prepared.text();
        boolean emptyInput = prepared.byteLength() == 0;
        IntUnaryOperator toCharIndex = toCharIndex(prepared);
        Set<Integer> tailIds = tailIdsFor(db);
        Map<Integer, int[]> startsById = sortedStartsByTailId(tailIds, toCharIndex);

        int count = collapsed.size();
        List<Match> matches = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int id = collapsed.id(i);
            long to = Math.max(collapsed.to(i), 1);
            Expression expression = db.getExpression(id);
            int startIndex = toCharIndex.applyAsInt((int) collapsed.from(i));
            int endIndex = toCharIndex.applyAsInt((int) to - 1);
            if (tailIds.contains(id)) {
                endIndex = extendToSentenceEnd(input, endIndex, startsById.get(id));
            }
            String matched = expression.getFlags().contains(ExpressionFlag.SOM_LEFTMOST)
                    ? input.substring(startIndex, endIndex + 1) : "";
            matches.add(emptyInput
                    ? new Match(0, 0, matched, expression)
                    : new Match(startIndex, endIndex, matched, expression));
        }
        return matches;
    }

    private static IntUnaryOperator toCharIndex(PreparedText prepared) {
        if (prepared.isAscii()) {
            return p -> p; // pure ASCII
        }
        int[] map = prepared.positionMap();
        return p -> map[p];
    }

    /** Per tail-id: sorted start offsets, so a tail never runs into the next occurrence. */
    private Map<Integer, int[]> sortedStartsByTailId(Set<Integer> tailIds, IntUnaryOperator toCharIndex) {
        if (tailIds.isEmpty()) {
            return Map.of();
        }
        Map<Integer, List<Integer>> grouped = new HashMap<>();
        for (int i = 0, n = collapsed.size(); i < n; i++) {
            int id = collapsed.id(i);
            if (tailIds.contains(id)) {
                grouped.computeIfAbsent(id, k -> new ArrayList<>())
                        .add(toCharIndex.applyAsInt((int) collapsed.from(i)));
            }
        }
        Map<Integer, int[]> result = new HashMap<>();
        grouped.forEach((k, v) -> result.put(k, v.stream().mapToInt(Integer::intValue).sorted().toArray()));
        return result;
    }

    private Database lastTailDb;
    private Set<Integer> lastTailIds = Set.of();

    /**
     * {@link TrailingDotStarRewriter#tailIds} allocates a weak-reference key per call; a scanner scans the same
     * database for many consecutive texts, so remember the last answer. A database's tail ids never change once
     * it is built, and the reference is dropped in {@link #close()}.
     */
    private Set<Integer> tailIdsFor(Database db) {
        if (db != lastTailDb) { // NOSONAR identity on purpose: Database.equals compares content, see TrailingDotStarRewriter.TAIL_IDS
            lastTailIds = TrailingDotStarRewriter.tailIds(db);
            lastTailDb = db;
        }
        return lastTailIds;
    }

    /** Default for {@link #maxTailChars}: a rewritten trailing {@code .*} never extends a match by more than this. */
    public static final int DEFAULT_MAX_TAIL_CHARS = 1000;

    /** The configured cap on how far a rewritten trailing {@code .*} extends a match. */
    public int maxTailChars() {
        return maxTailChars;
    }

    /**
     * Emulates the removed {@code .*} as "to the end of the line": up to (excluding) the first line break
     * ({@code \n} or {@code \r}) after the prefix, else to end of text — never beyond {@link #maxTailChars}
     * characters, and never into the next occurrence of the same expression. Sentence punctuation
     * ({@code . ! ?}) does NOT stop the tail.
     *
     * @return the new INCLUSIVE end index
     */
    private int extendToSentenceEnd(String text, int prefixEndInclusive, int[] sortedStarts) {
        // long arithmetic: prefixEndInclusive + 1 + maxTailChars can overflow int for a huge configured cap
        int limit = (int) Math.min(text.length(), (long) prefixEndInclusive + 1 + maxTailChars); // exclusive
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
        }
        return end;
    }

    /**
     * Keeps, per {@code (expressionId, startByteOffset)}, the longest end offset Hyperscan reported — in first-seen
     * order — with primitive arrays only: open addressing (linear probing) over {@code long} keys, entries in
     * parallel arrays. Called from the native match callback, so {@link #add} allocates nothing unless it grows.
     * {@link #clear} drops oversized arrays so one match-heavy text doesn't pin memory for the scanner's lifetime.
     */
    static final class MatchCollector {
        private static final int INITIAL_TABLE = 64;   // power of two
        private static final int INITIAL_ENTRIES = 32;
        private static final int RETAIN_MAX_TABLE = 1 << 14;

        private long[] keys = new long[INITIAL_TABLE];
        private int[] slots = new int[INITIAL_TABLE];  // entry index + 1; 0 = empty
        private int[] ids = new int[INITIAL_ENTRIES];
        private long[] froms = new long[INITIAL_ENTRIES];
        private long[] tos = new long[INITIAL_ENTRIES];
        private int size;

        void add(int id, long from, long to) {
            long key = ((long) id << 32) | (from & 0xFFFFFFFFL);
            int mask = slots.length - 1;
            int h = mix(key) & mask;
            while (true) {
                int slot = slots[h];
                if (slot == 0) {
                    break;
                }
                if (keys[h] == key) {
                    int e = slot - 1;
                    if (to > tos[e]) {
                        tos[e] = to;
                    }
                    return;
                }
                h = (h + 1) & mask;
            }
            if (size == ids.length) {
                int n = size * 2;
                ids = Arrays.copyOf(ids, n);
                froms = Arrays.copyOf(froms, n);
                tos = Arrays.copyOf(tos, n);
            }
            ids[size] = id;
            froms[size] = from;
            tos[size] = to;
            size++;
            keys[h] = key;
            slots[h] = size;
            if (size * 2 > slots.length) { // load factor 0.5
                rehash(slots.length * 2);
            }
        }

        private void rehash(int capacity) {
            long[] newKeys = new long[capacity];
            int[] newSlots = new int[capacity];
            int mask = capacity - 1;
            for (int i = 0; i < slots.length; i++) {
                if (slots[i] != 0) {
                    int h = mix(keys[i]) & mask;
                    while (newSlots[h] != 0) {
                        h = (h + 1) & mask;
                    }
                    newKeys[h] = keys[i];
                    newSlots[h] = slots[i];
                }
            }
            keys = newKeys;
            slots = newSlots;
        }

        private static int mix(long key) {
            long h = key * 0x9E3779B97F4A7C15L;
            return (int) (h ^ (h >>> 32));
        }

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        int id(int i) {
            return ids[i];
        }

        long from(int i) {
            return froms[i];
        }

        long to(int i) {
            return tos[i];
        }

        void clear() {
            if (slots.length > RETAIN_MAX_TABLE) {
                keys = new long[INITIAL_TABLE];
                slots = new int[INITIAL_TABLE];
                ids = new int[INITIAL_ENTRIES];
                froms = new long[INITIAL_ENTRIES];
                tos = new long[INITIAL_ENTRIES];
            } else if (size > 0) {
                Arrays.fill(slots, 0); // keys need no reset: slot 0 marks a free bucket
            }
            size = 0;
        }
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
        lastTailDb = null;
        lastTailIds = Set.of();
    }
}
