package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * One row of {@code pipeline_stage_audit} — a stage-level lifecycle record.
 * Requirement 3.a: written once with {@code jobStatus = IN_PROGRESS} when
 * the job starts, and again with {@code SUCCESS}/{@code FAILED} on
 * completion. Two writes, two rows (this table accumulates history rather
 * than being updated in place, matching BigQuery's append-oriented write
 * model).
 *
 * <p>Field name note: {@link #pipelineExecId} maps to the BQ column
 * literally named {@code pipelinex_exec_id} (with an "x") — see
 * {@code BqColumns.PipelineStageAudit#PIPELINE_EXEC_ID}; kept verbatim from
 * the delivered schema rather than "corrected", since it persisted across a
 * schema revision and is therefore treated as intentional.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class PipelineStageAuditRow implements Serializable {

    private final String processId;
    private final String triggerType;
    private final String pipelineExecId;
    private final String stageName;
    private final String composerDagName;
    private final String composerDagPath;
    private final String dprocDagName;
    private final String dprocDagPath;
    private final Instant startTime;
    private final Instant endTime;
    private final String jobStatus;
    private final String errorCount;
    private final String errorMessage;
    private final String additionalInfo;
    private final LocalDate executionDate;

    /**
     * @param processId              the process run this stage belongs to
     * @param triggerType             {@code "policy-alert-live"} / {@code "policy-alert-test"}
     * @param pipelineExecId          the pipeline execution this stage belongs to
     * @param stageName                identifies this job/stage (e.g. {@code "lexicon-scan-engine"})
     * @param composerDagName        the triggering Composer DAG's name
     * @param composerDagPath         the triggering Composer DAG's path
     * @param dprocDagName             this Dataproc job's identifying name
     * @param dprocDagPath              this Dataproc job's script/jar path
     * @param startTime                  UTC
     * @param endTime                     UTC; null on the {@code IN_PROGRESS} row
     * @param jobStatus                  {@code IN_PROGRESS} / {@code SUCCESS} / {@code FAILED} —
     *                                  see {@code BqColumns.JobStatus}
     * @param errorCount                  STRING, matching the delivered schema's column type
     * @param errorMessage                 top-level failure summary, null on success
     * @param additionalInfo                free-form context (e.g. input/output row counts)
     * @param executionDate                 the logical run date this execution covers
     */
    public PipelineStageAuditRow(String processId, String triggerType, String pipelineExecId, String stageName,
                                  String composerDagName, String composerDagPath, String dprocDagName,
                                  String dprocDagPath, Instant startTime, Instant endTime, String jobStatus,
                                  String errorCount, String errorMessage, String additionalInfo,
                                  LocalDate executionDate) {
        this.processId = processId;
        this.triggerType = triggerType;
        this.pipelineExecId = pipelineExecId;
        this.stageName = stageName;
        this.composerDagName = composerDagName;
        this.composerDagPath = composerDagPath;
        this.dprocDagName = dprocDagName;
        this.dprocDagPath = dprocDagPath;
        this.startTime = startTime;
        this.endTime = endTime;
        this.jobStatus = jobStatus;
        this.errorCount = errorCount;
        this.errorMessage = errorMessage;
        this.additionalInfo = additionalInfo;
        this.executionDate = executionDate;
    }

    public String processId() { return processId; }
    public String triggerType() { return triggerType; }
    public String pipelineExecId() { return pipelineExecId; }
    public String stageName() { return stageName; }
    public String composerDagName() { return composerDagName; }
    public String composerDagPath() { return composerDagPath; }
    public String dprocDagName() { return dprocDagName; }
    public String dprocDagPath() { return dprocDagPath; }
    public Instant startTime() { return startTime; }
    public Instant endTime() { return endTime; }
    public String jobStatus() { return jobStatus; }
    public String errorCount() { return errorCount; }
    public String errorMessage() { return errorMessage; }
    public String additionalInfo() { return additionalInfo; }
    public LocalDate executionDate() { return executionDate; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PipelineStageAuditRow)) return false;
        PipelineStageAuditRow other = (PipelineStageAuditRow) o;
        return Objects.equals(processId, other.processId) && Objects.equals(triggerType, other.triggerType)
                && Objects.equals(pipelineExecId, other.pipelineExecId) && Objects.equals(stageName, other.stageName)
                && Objects.equals(composerDagName, other.composerDagName)
                && Objects.equals(composerDagPath, other.composerDagPath)
                && Objects.equals(dprocDagName, other.dprocDagName)
                && Objects.equals(dprocDagPath, other.dprocDagPath)
                && Objects.equals(startTime, other.startTime) && Objects.equals(endTime, other.endTime)
                && Objects.equals(jobStatus, other.jobStatus) && Objects.equals(errorCount, other.errorCount)
                && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(additionalInfo, other.additionalInfo)
                && Objects.equals(executionDate, other.executionDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, triggerType, pipelineExecId, stageName, composerDagName, composerDagPath,
                dprocDagName, dprocDagPath, startTime, endTime, jobStatus, errorCount, errorMessage,
                additionalInfo, executionDate);
    }

    @Override
    public String toString() {
        return "PipelineStageAuditRow[processId=" + processId + ", triggerType=" + triggerType
                + ", pipelineExecId=" + pipelineExecId + ", stageName=" + stageName
                + ", composerDagName=" + composerDagName + ", composerDagPath=" + composerDagPath
                + ", dprocDagName=" + dprocDagName + ", dprocDagPath=" + dprocDagPath
                + ", startTime=" + startTime + ", endTime=" + endTime + ", jobStatus=" + jobStatus
                + ", errorCount=" + errorCount + ", errorMessage=" + errorMessage
                + ", additionalInfo=" + additionalInfo + ", executionDate=" + executionDate + "]";
    }
}
