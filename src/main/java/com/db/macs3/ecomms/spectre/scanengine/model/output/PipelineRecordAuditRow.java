package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * One row of {@code pipeline_record_audit} — a per-RECORD (per-message)
 * outcome, written for messages that failed processing. A per-message
 * processing failure does not fail the whole job — it is recorded here
 * instead.
 *
 * <p>Field name note: {@link #pipelineExecId} maps to the BQ column
 * literally named {@code pipelinex_exec_id} — see
 * {@link PipelineStageAuditRow} class Javadoc for the same note.
 */
public final class PipelineRecordAuditRow implements Serializable {

    private final String processId;
    private final String triggerType;
    private final String pipelineExecId;
    private final String stageName;
    private final String recordId;
    private final String status;
    private final Integer returnCode;
    private final String errorMessage;
    private final LocalDate executionDate;
    private final String createdBy;
    private final Instant createdTs;

    /**
     * @param processId          the process run this record belongs to
     * @param triggerType         {@code "policy-alert-live"} / {@code "policy-alert-test"}
     * @param pipelineExecId      the pipeline execution this record belongs to
     * @param stageName            identifies this job/stage
     * @param recordId              the failing message's {@code message_id}
     * @param status                 {@code SUCCESS} / {@code FAILED} — see {@code BqColumns.RecordStatus}
     * @param returnCode             an integer failure code (job-defined)
     * @param errorMessage           the specific failure detail for this message
     * @param executionDate          the logical run date this execution covers
     * @param createdBy               the writing job's identity
     * @param createdTs                write time, UTC
     */
    public PipelineRecordAuditRow(String processId, String triggerType, String pipelineExecId, String stageName,
                                   String recordId, String status, Integer returnCode, String errorMessage,
                                   LocalDate executionDate, String createdBy, Instant createdTs) {
        this.processId = processId;
        this.triggerType = triggerType;
        this.pipelineExecId = pipelineExecId;
        this.stageName = stageName;
        this.recordId = recordId;
        this.status = status;
        this.returnCode = returnCode;
        this.errorMessage = errorMessage;
        this.executionDate = executionDate;
        this.createdBy = createdBy;
        this.createdTs = createdTs;
    }

    public String processId() { return processId; }
    public String triggerType() { return triggerType; }
    public String pipelineExecId() { return pipelineExecId; }
    public String stageName() { return stageName; }
    public String recordId() { return recordId; }
    public String status() { return status; }
    public Integer returnCode() { return returnCode; }
    public String errorMessage() { return errorMessage; }
    public LocalDate executionDate() { return executionDate; }
    public String createdBy() { return createdBy; }
    public Instant createdTs() { return createdTs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PipelineRecordAuditRow)) {
            return false;
        }
        PipelineRecordAuditRow other = (PipelineRecordAuditRow) o;
        return Objects.equals(processId, other.processId) && Objects.equals(triggerType, other.triggerType)
                && Objects.equals(pipelineExecId, other.pipelineExecId) && Objects.equals(stageName, other.stageName)
                && Objects.equals(recordId, other.recordId) && Objects.equals(status, other.status)
                && Objects.equals(returnCode, other.returnCode) && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(executionDate, other.executionDate) && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(createdTs, other.createdTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, triggerType, pipelineExecId, stageName, recordId, status,
                returnCode, errorMessage, executionDate, createdBy, createdTs);
    }

    @Override
    public String toString() {
        return "PipelineRecordAuditRow[processId=" + processId + ", triggerType=" + triggerType
                + ", pipelineExecId=" + pipelineExecId + ", stageName=" + stageName + ", recordId=" + recordId
                + ", status=" + status + ", returnCode=" + returnCode + ", errorMessage=" + errorMessage
                + ", executionDate=" + executionDate + ", createdBy=" + createdBy + ", createdTs=" + createdTs + "]";
    }
}
