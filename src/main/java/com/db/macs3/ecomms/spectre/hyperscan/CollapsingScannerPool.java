package com.db.macs3.ecomms.spectre.hyperscan;

import com.gliwka.hyperscan.wrapper.CollapsingScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Executor-wide (one per JVM) pool of long-lived {@link CollapsingScanner}s. Spark tasks borrow one at partition
 * start and give it back at partition end; scanners are NEVER closed.
 *
 * <h2>Why — the executor SIGSEGV / SIGABRT</h2>
 * Each scanner owns a JavaCPP {@code match_event_handler} callback. JavaCPP gives that callback type a fixed table of
 * 10 native slots per JVM, each mapped to one live Java handler. Creating and closing a scanner per Spark task (the
 * pre-pool design: thousands of times per executor, on several task threads at once) was reproduced to end, under
 * load, with a scan calling back into a slot that no longer has a Java handler behind it. JavaCPP then substitutes a
 * bare {@code match_event_handler} whose {@code call} re-enters the same native stub: infinite recursion, a
 * StackOverflowError inside the match callback, and a C++ exception thrown through Hyperscan's C frames, which kills
 * the JVM. It never happened with long-lived scanners, nor with many threads scanning one shared database
 * concurrently. Reusing a handful of scanners for the whole executor life removes the churn entirely: slots are
 * claimed once, by at most {@link #MAX_SCANNERS} scanners, and never released.
 *
 * <p>A pooled scanner is used by one task thread at a time (borrow/release are synchronized, giving the
 * happens-before edge between consecutive users). At most {@link #MAX_SCANNERS} exist in total; a borrower
 * beyond that waits for one to be returned, which only happens if a JVM runs more concurrent scanning tasks than that.
 */
public final class CollapsingScannerPool {

    private static final Logger log = LoggerFactory.getLogger(CollapsingScannerPool.class);

    /** Stays below JavaCPP's 10 match_event_handler slots per JVM, leaving room for any other scanner. */
    public static final int MAX_SCANNERS = 8;

    private static final CollapsingScannerPool EXECUTOR = new CollapsingScannerPool(MAX_SCANNERS);

    private final int maxScanners;
    private final Map<Integer, Deque<CollapsingScanner>> idleByTailCap = new HashMap<>(); // guarded by this
    private int created;                                                                   // guarded by this

    CollapsingScannerPool(int maxScanners) {
        if (maxScanners <= 0) {
            throw new IllegalArgumentException("maxScanners must be positive, got " + maxScanners);
        }
        this.maxScanners = maxScanners;
    }

    /** The executor JVM's single pool. */
    public static CollapsingScannerPool forExecutor() {
        return EXECUTOR;
    }

    /**
     * Borrows a scanner whose trailing-{@code .*} cap is {@code maxTailChars}, creating one if fewer than
     * {@link #MAX_SCANNERS} exist; otherwise waits for one to be returned. Must be given back with {@link #release}.
     */
    public synchronized CollapsingScanner borrow(int maxTailChars) {
        while (true) {
            Deque<CollapsingScanner> idle = idleByTailCap.get(maxTailChars);
            if (idle != null && !idle.isEmpty()) {
                return idle.pop();
            }
            if (created < maxScanners) {
                CollapsingScanner scanner = new CollapsingScanner(maxTailChars);
                created++;
                log.info("Created pooled Hyperscan scanner {}/{} (maxTailChars={})", created, maxScanners, maxTailChars);
                return scanner;
            }
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for a pooled Hyperscan scanner", e);
            }
        }
    }

    /** Returns a scanner obtained from {@link #borrow}. It stays open for the next task. */
    public synchronized void release(CollapsingScanner scanner) {
        scanner.forgetDatabases();
        idleByTailCap.computeIfAbsent(scanner.maxTailChars(), k -> new ArrayDeque<>()).push(scanner);
        notifyAll();
    }

    /** Scanners created so far (diagnostics/tests). */
    public synchronized int createdCount() {
        return created;
    }
}
