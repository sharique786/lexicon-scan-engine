package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * One row in the {@code spectre-audit.pipeline_record_audit} BigQuery table.
 *
 * <p>Like {@link PipelineStageAuditRow}, this is a SHARED audit table written
 * by multiple Dataproc jobs across the platform. The Lexicon Scan Engine
 * writes one row per message it processes, with
 * {@link #stageName} = {@code "spectre-lexicon-tagging"}[-test] and
 * {@link #msgOutputFileType} = {@code "Lexicon-tagging"}.
 *
 * <p>Schema (columns confirmed from platform audit table screenshots):
 * <pre>
 * process_id            STRING NOT NULL
 * trigger_type          STRING
 * eval_test_id          STRING
 * pipeline_exec_id      STRING NOT NULL
 * stage_name            STRING NOT NULL
 * record_id             STRING             (message_id for this engine)
 * msg_input_file_nm     STRING             (AVRO file name / "messages.avro" or "&lt;msg-id&gt;")
 * msg_input_file_path   STRING             (GCS path to the input message file)
 * msg_output_file_type  STRING             ("Lexicon-tagging")
 * msg_output_file_nm    STRING             (output table reference)
 * output_file           STRING             (output GCS/BQ dataset reference)
 * status                STRING             ("SUCCESS" | "FAILED")
 * return_code           STRING
 * error_message         STRING             (e.g. "if any msg -- unable to parse")
 * created_ts            TIMESTAMP
 * created_by            STRING
 * </pre>
 */
public class PipelineRecordAuditRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED  = "FAILED";
    public static final String OUTPUT_TYPE_LEXICON_TAGGING = "Lexicon-tagging";

    private String processId;
    private String triggerType;
    private String evalTestId;
    private String pipelineExecId;
    private String stageName;
    private String recordId;
    private String msgInputFileNm;
    private String msgInputFilePath;
    private String msgOutputFileType;
    private String msgOutputFileNm;
    private String outputFile;
    private String status;
    private String returnCode;
    private String errorMessage;
    private Timestamp createdTs;
    private String createdBy = "SYSTEM";

    public PipelineRecordAuditRow() {}

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
    public String getRecordId()                  { return recordId; }
    public void setRecordId(String v)            { this.recordId = v; }
    public String getMsgInputFileNm()            { return msgInputFileNm; }
    public void setMsgInputFileNm(String v)      { this.msgInputFileNm = v; }
    public String getMsgInputFilePath()          { return msgInputFilePath; }
    public void setMsgInputFilePath(String v)    { this.msgInputFilePath = v; }
    public String getMsgOutputFileType()         { return msgOutputFileType; }
    public void setMsgOutputFileType(String v)   { this.msgOutputFileType = v; }
    public String getMsgOutputFileNm()           { return msgOutputFileNm; }
    public void setMsgOutputFileNm(String v)     { this.msgOutputFileNm = v; }
    public String getOutputFile()                { return outputFile; }
    public void setOutputFile(String v)          { this.outputFile = v; }
    public String getStatus()                    { return status; }
    public void setStatus(String v)              { this.status = v; }
    public String getReturnCode()                { return returnCode; }
    public void setReturnCode(String v)          { this.returnCode = v; }
    public String getErrorMessage()              { return errorMessage; }
    public void setErrorMessage(String v)        { this.errorMessage = v; }
    public Timestamp getCreatedTs()              { return createdTs; }
    public void setCreatedTs(Timestamp v)        { this.createdTs = v; }
    public String getCreatedBy()                 { return createdBy; }
    public void setCreatedBy(String v)           { this.createdBy = v; }

    @Override
    public String toString() {
        return "PipelineRecordAuditRow{recordId='" + recordId + "', status='" + status + "'}";
    }
}
