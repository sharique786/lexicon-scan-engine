package com.db.macs3.ecomms.spectre.bq;

/**
 * How one message-scale table is written, sized from its row count (known before the write — see
 * {@code OutputRowCounts}).
 *
 * <ul>
 *   <li>{@code partitions}: the Dataset is {@code coalesce}d to this many partitions before writing (narrow, no
 *       shuffle; never increases the count). Each partition is one staged Avro file for an indirect write, or one
 *       Storage Write API stream for a direct write. Without it a table is written with the scan's partition count
 *       (1000+ after the join shuffle), i.e. thousands of tiny or empty files/streams for a table that may hold a
 *       few thousand rows (the detail tables only get rows for messages with hits).</li>
 *   <li>{@code loadJobs}: when greater than 1 the table is split by {@code message_id} hash into that many
 *       separate writes (separate BigQuery load jobs). Opt-in only ({@code scan-engine.bq-max-rows-per-load-job}),
 *       because it gives up the single-load-job atomicity: if load job k fails, jobs 1..k-1 are already committed
 *       and a rerun of this append-only job will duplicate them.</li>
 * </ul>
 */
public record BqWritePlan(long rows, int partitions, int loadJobs) {

    /** Nothing to write. */
    public static final BqWritePlan EMPTY = new BqWritePlan(0, 0, 0);

    /**
     * @param rowsPerPartition target rows per written partition (file/stream); must be &gt; 0
     * @param maxRowsPerLoadJob rows above which the write is split into several load jobs; &lt;= 0 disables
     *                          splitting
     */
    public static BqWritePlan forRows(long rows, int rowsPerPartition, long maxRowsPerLoadJob) {
        if (rowsPerPartition <= 0) {
            throw new IllegalArgumentException("rowsPerPartition must be > 0, got " + rowsPerPartition);
        }
        if (rows <= 0) {
            return EMPTY;
        }
        int loadJobs = maxRowsPerLoadJob > 0 ? (int) Math.min(Integer.MAX_VALUE, ceilDiv(rows, maxRowsPerLoadJob)) : 1;
        int partitions = (int) Math.min(Integer.MAX_VALUE, ceilDiv(rows, rowsPerPartition));
        return new BqWritePlan(rows, partitions, loadJobs);
    }

    public boolean isEmpty() {
        return rows <= 0;
    }

    private static long ceilDiv(long a, long b) {
        return (a + b - 1) / b;
    }
}
