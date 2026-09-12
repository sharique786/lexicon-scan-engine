package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.OutputTableWriter;
import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.model.output.PipelineRecordAuditRow;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.Serial;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Maps every {@link MessageProcessingResult} in a partition — success and
 * failure alike — to a {@code pipeline_record_audit} {@link Row}, so the
 * audit table reflects every record this job processed, each with its own
 * {@code SUCCESS}/{@code FAILED} {@link com.db.macs3.ecomms.spectre.constants.BqColumns.RecordStatus}
 * — see {@link ScanEngineJobRunner writeOutputs}, which runs this via
 * {@code Dataset.mapPartitions} directly on the cached results
 * {@code Dataset}, rather than via {@code JavaRDD.map}.
 *
 * <p>Only processId/triggerType/pipelineExecId/recordId/stageName/status/
 * returnCode/errorMessage/executionDate/createdBy/createdTs are populated —
 * every other field (rule evaluation details, token counts, Gemini request
 * timing, rerun/eval-test linkage) belongs to stages this job doesn't run
 * and has no source data for. The Integer count/token fields default to 0
 * rather than null since they have no real value to report here.
 * {@code returnCode}/{@code errorMessage} are null on a successful result.
 *
 * <p>{@code runtimeArgs}/{@code stageName}/{@code createdBy}/{@code executionDate}
 * are held as fields here (all genuinely {@link java.io.Serializable}) rather
 * than being read from {@code ScanEngineJobRunner}'s own fields inside a
 * lambda — referencing an instance field like {@code properties.getStageName()}
 * directly inside a closure passed to Spark would implicitly capture the
 * enclosing {@code ScanEngineJobRunner} itself (a driver-only Spring
 * {@code @Service}, not serializable), which previously caused a real
 * {@code Task not serializable} failure on a live Dataproc run. A standalone
 * class with only serializable fields avoids that class of bug entirely.
 */
public final class PipelineRecordAuditRowMapper implements MapPartitionsFunction<MessageProcessingResult, Row> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final RuntimeArgs runtimeArgs;
    private final String stageName;
    private final String createdBy;
    private final LocalDate executionDate;

    public PipelineRecordAuditRowMapper(RuntimeArgs runtimeArgs, String stageName, String createdBy,
                                        LocalDate executionDate) {
        this.runtimeArgs = runtimeArgs;
        this.stageName = stageName;
        this.createdBy = createdBy;
        this.executionDate = executionDate;
    }

    @Override
    public Iterator<Row> call(Iterator<MessageProcessingResult> input) {
        List<Row> rows = new ArrayList<>();
        while (input.hasNext()) {
            MessageProcessingResult result = input.next();
            boolean isError = result.isError();
            rows.add(OutputTableWriter.toRow(new PipelineRecordAuditRow(
                    runtimeArgs.processId(), runtimeArgs.triggerType(), null, runtimeArgs.pipelineExecId(),
                    result.getMessageId(), stageName, null, null, null, null, null,
                    isError ? BqColumns.RecordStatus.FAILED : BqColumns.RecordStatus.SUCCESS,
                    isError ? 1 : 0, result.getErrorMessage(), null, 0, null, 0,
                    0, 0, 0, 0, 0, 0,
                    Instant.now(), createdBy, null, executionDate, null, null, null, null, null, null)));
        }
        return rows.iterator();
    }
}
