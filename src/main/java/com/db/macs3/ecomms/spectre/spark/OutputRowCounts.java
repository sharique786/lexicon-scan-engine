package com.db.macs3.ecomms.spectre.spark;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Rows each output table will receive, counted in ONE pass over the (persisted) scan results. That pass is also the
 * action that runs the scan, so a scan failure is reported as a scan failure instead of surfacing inside the first
 * BigQuery write, and every write afterwards reads the persisted results.
 *
 * <p>Counts mirror the row mappers exactly: {@link SummaryRowMapper}/{@link FeatureHitSummaryRowMapper} emit one
 * row per non-error result, {@link DetailRowMapper} one per non-error result with a detail row of its own
 * restriction, {@link PipelineRecordAuditRowMapper} one per result (before de-duplication — an upper bound).
 *
 * <p><b>Deliberately NOT a record.</b> Each partition's fold result is a Spark task result, serialized with the job's
 * serializer — Kryo (see {@link SparkSessionConfig}). Spark's Kryo 4 {@code FieldSerializer} uses
 * {@code Unsafe.objectFieldOffset}, which the JDK refuses on record classes ("can't get field offset on a record
 * class"), so a record here failed every scan on the cluster. Any class that crosses a Kryo boundary (task result,
 * {@code Encoders.kryo} payload, broadcast value) must be a plain class, not a record.
 */
public final class OutputRowCounts implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long results;
    private final long errors;
    private final long restrictedDetail;
    private final long unrestrictedDetail;

    public OutputRowCounts(long results, long errors, long restrictedDetail, long unrestrictedDetail) {
        this.results = results;
        this.errors = errors;
        this.restrictedDetail = restrictedDetail;
        this.unrestrictedDetail = unrestrictedDetail;
    }

    public long results() {
        return results;
    }

    public long errors() {
        return errors;
    }

    public long restrictedDetail() {
        return restrictedDetail;
    }

    public long unrestrictedDetail() {
        return unrestrictedDetail;
    }

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

    @Override
    public boolean equals(Object o) {
        return o instanceof OutputRowCounts other && results == other.results && errors == other.errors
                && restrictedDetail == other.restrictedDetail && unrestrictedDetail == other.unrestrictedDetail;
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, errors, restrictedDetail, unrestrictedDetail);
    }

    @Override
    public String toString() {
        return "OutputRowCounts[results=" + results + ", errors=" + errors + ", restrictedDetail=" + restrictedDetail
                + ", unrestrictedDetail=" + unrestrictedDetail + "]";
    }
}
