package com.db.macs3.ecomms.spectre.spark;

import java.io.Serial;
import java.io.Serializable;

/**
 * Rows each output table will receive, counted in ONE pass over the (persisted) scan results. That pass is also the
 * action that runs the scan, so a scan failure is reported as a scan failure instead of surfacing inside the first
 * BigQuery write, and every write afterwards reads the persisted results.
 *
 * <p>Counts mirror the row mappers exactly: {@link SummaryRowMapper}/{@link FeatureHitSummaryRowMapper} emit one
 * row per non-error result, {@link DetailRowMapper} one per non-error result with a detail row of its own
 * restriction, {@link PipelineRecordAuditRowMapper} one per result (before de-duplication — an upper bound).
 */
public record OutputRowCounts(long results, long errors, long restrictedDetail, long unrestrictedDetail)
        implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final OutputRowCounts ZERO = new OutputRowCounts(0, 0, 0, 0);

    public static OutputRowCounts of(MessageProcessingResult result) {
        if (result.isError()) {
            return new OutputRowCounts(1, 1, 0, 0);
        }
        boolean hasDetail = result.getDetailRow() != null;
        return new OutputRowCounts(1, 0,
                hasDetail && result.isRestricted() ? 1 : 0,
                hasDetail && !result.isRestricted() ? 1 : 0);
    }

    public OutputRowCounts plus(OutputRowCounts other) {
        return new OutputRowCounts(results + other.results, errors + other.errors,
                restrictedDetail + other.restrictedDetail, unrestrictedDetail + other.unrestrictedDetail);
    }

    /** lexicon-hit-summary and feature-hit-summary: one row per successfully processed message. */
    public long summary() {
        return results - errors;
    }

    /** pipeline_record_audit, before de-duplication on its natural key. */
    public long recordAudit() {
        return results;
    }
}
