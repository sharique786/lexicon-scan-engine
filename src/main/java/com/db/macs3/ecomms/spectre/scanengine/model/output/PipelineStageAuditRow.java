package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * One row of {@code pipeline_stage_audit} — a stage-level lifecycle record.
 * Written once with {@code jobStatus = IN_PROGRESS} when the job starts, and
 * again with {@code SUCCESS}/{@code FAILED} on completion. Two writes, two
 * rows (this table accumulates history rather than being updated in place,
 * matching BigQuery's append-oriented write model).
 *
 * <p>Field name note: {@link #getPipelineExecId} maps to the BQ column
 * literally named {@code pipelinex_exec_id} (with an "x") — see
 * {@code BqColumns.PipelineStageAudit#PIPELINE_EXEC_ID}; kept verbatim from
 * the delivered schema rather than "corrected", since it persisted across a
 * schema revision and is therefore treated as intentional.
 *
 * <p><b>Schema note:</b> {@link #getDprocScriptName}/
 * {@link #getDprocScriptPath} were renamed from {@code dprocDagName}/
 * {@code dprocDagPath} (Dataproc runs a script, not a DAG — the DAG is
 * Composer's), and the delivered schema marks {@link #getComposerDagName},
 * {@link #getComposerDagPath}, {@link #getDprocScriptName}, and
 * {@link #getDprocScriptPath} all NOT NULL (previously nullable). Neither
 * {@code RuntimeArgs} nor {@code DataprocConfig} currently carries values for
 * any of these four fields — {@code ScanEngineJobRunner.writeStageAudit}
 * still passes {@code null} for all of them. This is a known, pre-existing
 * gap: a real BigQuery table enforcing NOT NULL on these columns would
 * reject the write. Wiring real Composer DAG name/path and the Dataproc
 * script name/path through to this row is not yet done.
 */
public class PipelineStageAuditRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String processId;
    private String triggerType;
    private String evalTestId;
    private String pipelineExecId;
    private String stageName;
    private String composerDagName;
    private String composerDagPath;
    private String dprocScriptName;
    private String dprocScriptPath;
    private ModelConfigDtls modelConfigDtls;
    private Instant startTime;
    private Instant endTime;
    private String jobStatus;
    private Integer inputFileCount;
    private Integer outputFileCount;
    private Integer inputRecordCount;
    private Integer outputRecordCount;
    private Integer errorCount;
    private String errorMessage;
    private String additionalInfo;
    private String logPath;
    private LocalDate executionDate;
    private String rerunFlg;
    private String rerunType;
    private String rerunProcessId;

    /**
     * @param processId              the process run this stage belongs to
     * @param triggerType             {@code "policy-alert-live"} / {@code "policy-alert-test"}
     * @param evalTestId              the evaluation/test run this stage belongs to, when triggered
     *                                 as part of one; null otherwise
     * @param pipelineExecId          the pipeline execution this stage belongs to
     * @param stageName                identifies this job/stage (e.g. {@code "lexicon-scan-engine"})
     * @param composerDagName        the triggering Composer DAG's name
     * @param composerDagPath         the triggering Composer DAG's path
     * @param dprocScriptName          this Dataproc job's identifying script/jar name
     * @param dprocScriptPath           this Dataproc job's script/jar path
     * @param modelConfigDtls           the generative-model configuration this stage ran with, when
     *                                  this stage invokes one; null otherwise (see {@link ModelConfigDtls})
     * @param startTime                  UTC
     * @param endTime                     UTC; null on the {@code IN_PROGRESS} row
     * @param jobStatus                  {@code IN_PROGRESS} / {@code SUCCESS} / {@code FAILED} —
     *                                  see {@code BqColumns.JobStatus}
     * @param inputFileCount              number of input files this stage processed
     * @param outputFileCount             number of output files this stage produced
     * @param inputRecordCount            number of input records this stage processed
     * @param outputRecordCount           number of output records this stage produced
     * @param errorCount                  INTEGER, matching the delivered schema's column type (previously STRING)
     * @param errorMessage                 top-level failure summary, null on success
     * @param additionalInfo                free-form context (e.g. input/output row counts)
     * @param logPath                       GCS path to this stage's run log, when available
     * @param executionDate                 the logical run date this execution covers
     * @param rerunFlg                       whether this row represents a rerun of a previous execution
     * @param rerunType                      the kind of rerun, when {@code rerunFlg} is set
     * @param rerunProcessId                 the original {@code processId} being rerun, when {@code rerunFlg} is set
     */
    public PipelineStageAuditRow(String processId, String triggerType, String evalTestId, String pipelineExecId,
                                  String stageName, String composerDagName, String composerDagPath,
                                  String dprocScriptName, String dprocScriptPath, ModelConfigDtls modelConfigDtls,
                                  Instant startTime, Instant endTime, String jobStatus, Integer inputFileCount,
                                  Integer outputFileCount, Integer inputRecordCount, Integer outputRecordCount,
                                  Integer errorCount, String errorMessage, String additionalInfo, String logPath,
                                  LocalDate executionDate, String rerunFlg, String rerunType, String rerunProcessId) {
        this.processId = processId;
        this.triggerType = triggerType;
        this.evalTestId = evalTestId;
        this.pipelineExecId = pipelineExecId;
        this.stageName = stageName;
        this.composerDagName = composerDagName;
        this.composerDagPath = composerDagPath;
        this.dprocScriptName = dprocScriptName;
        this.dprocScriptPath = dprocScriptPath;
        this.modelConfigDtls = modelConfigDtls;
        this.startTime = startTime;
        this.endTime = endTime;
        this.jobStatus = jobStatus;
        this.inputFileCount = inputFileCount;
        this.outputFileCount = outputFileCount;
        this.inputRecordCount = inputRecordCount;
        this.outputRecordCount = outputRecordCount;
        this.errorCount = errorCount;
        this.errorMessage = errorMessage;
        this.additionalInfo = additionalInfo;
        this.logPath = logPath;
        this.executionDate = executionDate;
        this.rerunFlg = rerunFlg;
        this.rerunType = rerunType;
        this.rerunProcessId = rerunProcessId;
    }

    public String getProcessId() { return processId; }
    public void setProcessId(String processId) { this.processId = processId; }
    public String getTriggerType() { return triggerType; }
    public void setTriggerType(String triggerType) { this.triggerType = triggerType; }
    public String getEvalTestId() { return evalTestId; }
    public void setEvalTestId(String evalTestId) { this.evalTestId = evalTestId; }
    public String getPipelineExecId() { return pipelineExecId; }
    public void setPipelineExecId(String pipelineExecId) { this.pipelineExecId = pipelineExecId; }
    public String getStageName() { return stageName; }
    public void setStageName(String stageName) { this.stageName = stageName; }
    public String getComposerDagName() { return composerDagName; }
    public void setComposerDagName(String composerDagName) { this.composerDagName = composerDagName; }
    public String getComposerDagPath() { return composerDagPath; }
    public void setComposerDagPath(String composerDagPath) { this.composerDagPath = composerDagPath; }
    public String getDprocScriptName() { return dprocScriptName; }
    public void setDprocScriptName(String dprocScriptName) { this.dprocScriptName = dprocScriptName; }
    public String getDprocScriptPath() { return dprocScriptPath; }
    public void setDprocScriptPath(String dprocScriptPath) { this.dprocScriptPath = dprocScriptPath; }
    public ModelConfigDtls getModelConfigDtls() { return modelConfigDtls; }
    public void setModelConfigDtls(ModelConfigDtls modelConfigDtls) { this.modelConfigDtls = modelConfigDtls; }
    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }
    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }
    public String getJobStatus() { return jobStatus; }
    public void setJobStatus(String jobStatus) { this.jobStatus = jobStatus; }
    public Integer getInputFileCount() { return inputFileCount; }
    public void setInputFileCount(Integer inputFileCount) { this.inputFileCount = inputFileCount; }
    public Integer getOutputFileCount() { return outputFileCount; }
    public void setOutputFileCount(Integer outputFileCount) { this.outputFileCount = outputFileCount; }
    public Integer getInputRecordCount() { return inputRecordCount; }
    public void setInputRecordCount(Integer inputRecordCount) { this.inputRecordCount = inputRecordCount; }
    public Integer getOutputRecordCount() { return outputRecordCount; }
    public void setOutputRecordCount(Integer outputRecordCount) { this.outputRecordCount = outputRecordCount; }
    public Integer getErrorCount() { return errorCount; }
    public void setErrorCount(Integer errorCount) { this.errorCount = errorCount; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getAdditionalInfo() { return additionalInfo; }
    public void setAdditionalInfo(String additionalInfo) { this.additionalInfo = additionalInfo; }
    public String getLogPath() { return logPath; }
    public void setLogPath(String logPath) { this.logPath = logPath; }
    public LocalDate getExecutionDate() { return executionDate; }
    public void setExecutionDate(LocalDate executionDate) { this.executionDate = executionDate; }
    public String getRerunFlg() { return rerunFlg; }
    public void setRerunFlg(String rerunFlg) { this.rerunFlg = rerunFlg; }
    public String getRerunType() { return rerunType; }
    public void setRerunType(String rerunType) { this.rerunType = rerunType; }
    public String getRerunProcessId() { return rerunProcessId; }
    public void setRerunProcessId(String rerunProcessId) { this.rerunProcessId = rerunProcessId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PipelineStageAuditRow)) {
            return false;
        }
        PipelineStageAuditRow other = (PipelineStageAuditRow) o;
        return Objects.equals(processId, other.processId) && Objects.equals(triggerType, other.triggerType)
                && Objects.equals(evalTestId, other.evalTestId) && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(stageName, other.stageName)
                && Objects.equals(composerDagName, other.composerDagName)
                && Objects.equals(composerDagPath, other.composerDagPath)
                && Objects.equals(dprocScriptName, other.dprocScriptName)
                && Objects.equals(dprocScriptPath, other.dprocScriptPath)
                && Objects.equals(modelConfigDtls, other.modelConfigDtls)
                && Objects.equals(startTime, other.startTime) && Objects.equals(endTime, other.endTime)
                && Objects.equals(jobStatus, other.jobStatus)
                && Objects.equals(inputFileCount, other.inputFileCount)
                && Objects.equals(outputFileCount, other.outputFileCount)
                && Objects.equals(inputRecordCount, other.inputRecordCount)
                && Objects.equals(outputRecordCount, other.outputRecordCount)
                && Objects.equals(errorCount, other.errorCount)
                && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(additionalInfo, other.additionalInfo)
                && Objects.equals(logPath, other.logPath)
                && Objects.equals(executionDate, other.executionDate)
                && Objects.equals(rerunFlg, other.rerunFlg)
                && Objects.equals(rerunType, other.rerunType)
                && Objects.equals(rerunProcessId, other.rerunProcessId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, triggerType, evalTestId, pipelineExecId, stageName, composerDagName,
                composerDagPath, dprocScriptName, dprocScriptPath, modelConfigDtls, startTime, endTime, jobStatus,
                inputFileCount, outputFileCount, inputRecordCount, outputRecordCount, errorCount, errorMessage,
                additionalInfo, logPath, executionDate, rerunFlg, rerunType, rerunProcessId);
    }

    @Override
    public String toString() {
        return "PipelineStageAuditRow[processId=" + processId + ", triggerType=" + triggerType
                + ", evalTestId=" + evalTestId + ", pipelineExecId=" + pipelineExecId + ", stageName=" + stageName
                + ", composerDagName=" + composerDagName + ", composerDagPath=" + composerDagPath
                + ", dprocScriptName=" + dprocScriptName + ", dprocScriptPath=" + dprocScriptPath
                + ", modelConfigDtls=" + modelConfigDtls + ", startTime=" + startTime + ", endTime=" + endTime
                + ", jobStatus=" + jobStatus + ", inputFileCount=" + inputFileCount
                + ", outputFileCount=" + outputFileCount + ", inputRecordCount=" + inputRecordCount
                + ", outputRecordCount=" + outputRecordCount + ", errorCount=" + errorCount
                + ", errorMessage=" + errorMessage + ", additionalInfo=" + additionalInfo + ", logPath=" + logPath
                + ", executionDate=" + executionDate + ", rerunFlg=" + rerunFlg + ", rerunType=" + rerunType
                + ", rerunProcessId=" + rerunProcessId + "]";
    }
}
