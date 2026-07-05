package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * One row in the {@code spectre-audit.pipeline_stage_audit} BigQuery table.
 *
 * <p>This is a SHARED audit table written by multiple Dataproc jobs across
 * the eCOMMS platform (metadata tagging, language-feature-decision, lexicon
 * tagging, evaluation tagging, alert processing, etc.) — the Lexicon Scan
 * Engine writes exactly one row per job execution with
 * {@link #stageName} = {@code "spectre-lexicon-tagging"} (or
 * {@code "spectre-lexicon-tagging-test"} for {@code POLICY_TEST} trigger
 * runs), leaving columns owned by other stages blank/null.
 *
 * <p>Schema (columns confirmed from platform audit table screenshots):
 * <pre>
 * process_id           STRING NOT NULL
 * trigger_type         STRING             (e.g. "policy-alert-test", "policy-alert-live")
 * eval_test_id         STRING             (blank for live/production runs)
 * pipeline_exec_id     STRING NOT NULL
 * stage_name           STRING NOT NULL    ("spectre-lexicon-tagging"[-test])
 * compsr_dag_name      STRING             (Composer/Airflow DAG name)
 * compsr_dag_path      STRING             (Composer/Airflow DAG GCS path)
 * dproc_script_name    STRING             (this job's jar/script name)
 * dproc_script_path    STRING             (this job's jar/script GCS path)
 * start_time           TIMESTAMP
 * end_time             TIMESTAMP
 * job_status           STRING             ("SUCCESS" | "FAILED")
 * input_file_count     INT64
 * output_file_count    INT64
 * record_cnt           INT64
 * error_count          INT64
 * error_message        STRING             (null on success)
 * </pre>
 */
public class PipelineStageAuditRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED  = "FAILED";

    private String processId;
    private String triggerType;
    private String evalTestId;
    private String pipelineExecId;
    private String stageName;
    private String compsrDagName;
    private String compsrDagPath;
    private String dprocScriptName;
    private String dprocScriptPath;
    private Timestamp startTime;
    private Timestamp endTime;
    private String jobStatus;
    private long inputFileCount;
    private long outputFileCount;
    private long recordCnt;
    private long errorCount;
    private String errorMessage;

    public PipelineStageAuditRow() {}

    public String getProcessId()                 { return processId; }
    public void setProcessId(String v)           { this.processId = v; }
    public String getTriggerType()               { return triggerType; }
    public void setTriggerType(String v)         { this.triggerType = v; }
    public String getEvalTestId()                { return evalTestId; }
    public void setEvalTestId(String v)          { this.evalTestId = v; }
    public String getPipelineExecId()            { return pipelineExecId; }
    public void setPipelineExecId(String v)      { this.pipelineExecId = v; }
    public String getStageName()                 { return stageName; }
    public void setStageName(String v)           { this.stageName = v; }
    public String getCompsrDagName()             { return compsrDagName; }
    public void setCompsrDagName(String v)       { this.compsrDagName = v; }
    public String getCompsrDagPath()             { return compsrDagPath; }
    public void setCompsrDagPath(String v)       { this.compsrDagPath = v; }
    public String getDprocScriptName()           { return dprocScriptName; }
    public void setDprocScriptName(String v)     { this.dprocScriptName = v; }
    public String getDprocScriptPath()           { return dprocScriptPath; }
    public void setDprocScriptPath(String v)     { this.dprocScriptPath = v; }
    public Timestamp getStartTime()              { return startTime; }
    public void setStartTime(Timestamp v)        { this.startTime = v; }
    public Timestamp getEndTime()                { return endTime; }
    public void setEndTime(Timestamp v)          { this.endTime = v; }
    public String getJobStatus()                 { return jobStatus; }
    public void setJobStatus(String v)           { this.jobStatus = v; }
    public long getInputFileCount()              { return inputFileCount; }
    public void setInputFileCount(long v)        { this.inputFileCount = v; }
    public long getOutputFileCount()             { return outputFileCount; }
    public void setOutputFileCount(long v)       { this.outputFileCount = v; }
    public long getRecordCnt()                   { return recordCnt; }
    public void setRecordCnt(long v)             { this.recordCnt = v; }
    public long getErrorCount()                  { return errorCount; }
    public void setErrorCount(long v)            { this.errorCount = v; }
    public String getErrorMessage()              { return errorMessage; }
    public void setErrorMessage(String v)        { this.errorMessage = v; }

    @Override
    public String toString() {
        return "PipelineStageAuditRow{pipelineExecId='" + pipelineExecId + "', stageName='" + stageName +
               "', jobStatus='" + jobStatus + "'}";
    }
}
