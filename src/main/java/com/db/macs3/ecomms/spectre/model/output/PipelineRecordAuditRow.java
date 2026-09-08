package com.db.macs3.ecomms.spectre.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * One row of {@code pipeline_record_audit} — a per-RECORD (per-message)
 * outcome, written for every message this job processes, whether it
 * succeeded or failed. A per-message processing failure does not fail the
 * whole job — it is recorded here, alongside every successful record, with
 * its own {@code SUCCESS}/{@code FAILED} status instead.
 *
 * <p>Field name note: {@link #getPipelineExecId} maps to the BQ column
 * literally named {@code pipeline_exec_id} — see
 * {@link PipelineStageAuditRow} class Javadoc for the same note.
 *
 * <p>Field name note: {@link #getMsgMatchTextTokens} maps to
 * the BQ column literally named {@code msg_attach_text_tokens} — "Match" in
 * the Java field, "Attach" in the delivered column — and {@link #getAdditionInfo}
 * (not "additional") likewise matches its column, {@code addition_info},
 * verbatim. Both kept as delivered rather than "corrected", same convention
 * as {@code pipeline_exec_id}.
 *
 * <p>Field ORDER note: {@link #getRecordId} now precedes {@link #getStageName} —
 * the delivered schema reordered these relative to an earlier revision of
 * this table (and of {@link PipelineStageAuditRow}, where {@code stageName}
 * still precedes any per-record field). This class's constructor and
 * {@code OutputTableWriter}'s {@code toRow}/schema all follow the new order;
 * do not "fix" it back to match {@code PipelineStageAuditRow}'s ordering.
 */
public class PipelineRecordAuditRow implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String processId;
    private String triggerType;
    private String evalTestId;
    private String pipelineExecId;
    private String recordId;
    private String stageName;
    private String msgInputFileNm;
    private String msgInputFilePath;
    private String msgOutputFilePath;
    private String msgOutputFileType;
    private String msgOutputFileNm;
    private String status;
    private Integer returnCode;
    private String errorMessage;
    private List<RuleDtlType> evaluatedRulesDtls;
    private Integer evaluatedRulesCnt;
    private List<RuleDtlType> detectedRulesDtls;
    private Integer detectedRulesCnt;
    private Integer sysPromptEvalRulesTokens;
    private Integer inputTokens;
    private Integer outputTokens;
    private Integer thinkingTokens;
    private Integer cachedTokens;
    private Integer msgMatchTextTokens;
    private Instant createdTs;
    private String createdBy;
    private String additionInfo;
    private LocalDate executionDate;
    private Instant sentDate;
    private String runDate;
    private String sourceName;
    private Instant geminiRequestStartTime;
    private Instant geminiRequestEndTime;
    private String rerunProcessId;

    /**
     * @param processId                the process run this record belongs to
     * @param triggerType              {@code "policy-alert-live"} / {@code "policy-alert-test"}
     * @param evalTestId               the evaluation/test run this record belongs to, when triggered
     *                                 as part of one; null otherwise
     * @param pipelineExecId           the pipeline execution this record belongs to
     * @param recordId                 the message's {@code message_id}
     * @param stageName                identifies this job/stage
     * @param msgInputFileNm           the input AVRO file's name this message was read from
     * @param msgInputFilePath         the input AVRO file's GCS path
     * @param msgOutputFilePath        the output file's GCS path, when this record produced one
     * @param msgOutputFileType        the output file's type/format
     * @param msgOutputFileNm          the output file's name
     * @param status                   {@code SUCCESS} / {@code FAILED} — see {@code BqColumns.RecordStatus}
     * @param returnCode               an integer failure code (job-defined)
     * @param errorMessage             the specific failure detail for this message
     * @param evaluatedRulesDtls       every rule this record was evaluated against
     * @param evaluatedRulesCnt        {@code evaluatedRulesDtls}'s size, carried separately
     * @param detectedRulesDtls        every rule this record actually matched/triggered
     * @param detectedRulesCnt         {@code detectedRulesDtls}'s size, carried separately
     * @param sysPromptEvalRulesTokens token count for the system-prompt rule evaluation, when applicable
     * @param inputTokens              model input token count, when applicable
     * @param outputTokens             model output token count, when applicable
     * @param thinkingTokens           model thinking/reasoning token count, when applicable
     * @param cachedTokens             model cached-prompt token count, when applicable
     * @param msgMatchTextTokens       maps to BQ column {@code msg_attach_text_tokens} — see class Javadoc
     * @param createdTs                write time, UTC
     * @param createdBy                the writing job's identity
     * @param additionInfo             free-form context; maps to BQ column {@code addition_info} — see class Javadoc
     * @param executionDate            the logical run date this execution covers
     * @param sentDate                 when this record was sent downstream, when applicable
     * @param runDate                  the logical run date as delivered upstream (STRING, matching the delivered schema's column type)
     * @param sourceName               the upstream source system's identifier
     * @param geminiRequestStartTime   start time of the model request this record triggered, when applicable
     * @param geminiRequestEndTime     end time of the model request this record triggered, when applicable
     * @param rerunProcessId           the original {@code processId} being rerun, when this record is part of a rerun
     */
    public PipelineRecordAuditRow(String processId, String triggerType, String evalTestId, String pipelineExecId,
                                  String recordId, String stageName, String msgInputFileNm, String msgInputFilePath,
                                  String msgOutputFilePath, String msgOutputFileType, String msgOutputFileNm,
                                  String status, Integer returnCode, String errorMessage,
                                  List<RuleDtlType> evaluatedRulesDtls, Integer evaluatedRulesCnt,
                                  List<RuleDtlType> detectedRulesDtls, Integer detectedRulesCnt,
                                  Integer sysPromptEvalRulesTokens, Integer inputTokens, Integer outputTokens,
                                  Integer thinkingTokens, Integer cachedTokens, Integer msgMatchTextTokens,
                                  Instant createdTs, String createdBy, String additionInfo, LocalDate executionDate,
                                  Instant sentDate, String runDate, String sourceName, Instant geminiRequestStartTime,
                                  Instant geminiRequestEndTime, String rerunProcessId) {
        this.processId = processId;
        this.triggerType = triggerType;
        this.evalTestId = evalTestId;
        this.pipelineExecId = pipelineExecId;
        this.recordId = recordId;
        this.stageName = stageName;
        this.msgInputFileNm = msgInputFileNm;
        this.msgInputFilePath = msgInputFilePath;
        this.msgOutputFilePath = msgOutputFilePath;
        this.msgOutputFileType = msgOutputFileType;
        this.msgOutputFileNm = msgOutputFileNm;
        this.status = status;
        this.returnCode = returnCode;
        this.errorMessage = errorMessage;
        this.evaluatedRulesDtls = evaluatedRulesDtls;
        this.evaluatedRulesCnt = evaluatedRulesCnt;
        this.detectedRulesDtls = detectedRulesDtls;
        this.detectedRulesCnt = detectedRulesCnt;
        this.sysPromptEvalRulesTokens = sysPromptEvalRulesTokens;
        this.inputTokens = inputTokens;
        this.outputTokens = outputTokens;
        this.thinkingTokens = thinkingTokens;
        this.cachedTokens = cachedTokens;
        this.msgMatchTextTokens = msgMatchTextTokens;
        this.createdTs = createdTs;
        this.createdBy = createdBy;
        this.additionInfo = additionInfo;
        this.executionDate = executionDate;
        this.sentDate = sentDate;
        this.runDate = runDate;
        this.sourceName = sourceName;
        this.geminiRequestStartTime = geminiRequestStartTime;
        this.geminiRequestEndTime = geminiRequestEndTime;
        this.rerunProcessId = rerunProcessId;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(String triggerType) {
        this.triggerType = triggerType;
    }

    public String getEvalTestId() {
        return evalTestId;
    }

    public void setEvalTestId(String evalTestId) {
        this.evalTestId = evalTestId;
    }

    public String getPipelineExecId() {
        return pipelineExecId;
    }

    public void setPipelineExecId(String pipelineExecId) {
        this.pipelineExecId = pipelineExecId;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public String getMsgInputFileNm() {
        return msgInputFileNm;
    }

    public void setMsgInputFileNm(String msgInputFileNm) {
        this.msgInputFileNm = msgInputFileNm;
    }

    public String getMsgInputFilePath() {
        return msgInputFilePath;
    }

    public void setMsgInputFilePath(String msgInputFilePath) {
        this.msgInputFilePath = msgInputFilePath;
    }

    public String getMsgOutputFilePath() {
        return msgOutputFilePath;
    }

    public void setMsgOutputFilePath(String msgOutputFilePath) {
        this.msgOutputFilePath = msgOutputFilePath;
    }

    public String getMsgOutputFileType() {
        return msgOutputFileType;
    }

    public void setMsgOutputFileType(String msgOutputFileType) {
        this.msgOutputFileType = msgOutputFileType;
    }

    public String getMsgOutputFileNm() {
        return msgOutputFileNm;
    }

    public void setMsgOutputFileNm(String msgOutputFileNm) {
        this.msgOutputFileNm = msgOutputFileNm;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(Integer returnCode) {
        this.returnCode = returnCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public List<RuleDtlType> getEvaluatedRulesDtls() {
        return evaluatedRulesDtls;
    }

    public void setEvaluatedRulesDtls(List<RuleDtlType> evaluatedRulesDtls) {
        this.evaluatedRulesDtls = evaluatedRulesDtls;
    }

    public Integer getEvaluatedRulesCnt() {
        return evaluatedRulesCnt;
    }

    public void setEvaluatedRulesCnt(Integer evaluatedRulesCnt) {
        this.evaluatedRulesCnt = evaluatedRulesCnt;
    }

    public List<RuleDtlType> getDetectedRulesDtls() {
        return detectedRulesDtls;
    }

    public void setDetectedRulesDtls(List<RuleDtlType> detectedRulesDtls) {
        this.detectedRulesDtls = detectedRulesDtls;
    }

    public Integer getDetectedRulesCnt() {
        return detectedRulesCnt;
    }

    public void setDetectedRulesCnt(Integer detectedRulesCnt) {
        this.detectedRulesCnt = detectedRulesCnt;
    }

    public Integer getSysPromptEvalRulesTokens() {
        return sysPromptEvalRulesTokens;
    }

    public void setSysPromptEvalRulesTokens(Integer sysPromptEvalRulesTokens) {
        this.sysPromptEvalRulesTokens = sysPromptEvalRulesTokens;
    }

    public Integer getInputTokens() {
        return inputTokens;
    }

    public void setInputTokens(Integer inputTokens) {
        this.inputTokens = inputTokens;
    }

    public Integer getOutputTokens() {
        return outputTokens;
    }

    public void setOutputTokens(Integer outputTokens) {
        this.outputTokens = outputTokens;
    }

    public Integer getThinkingTokens() {
        return thinkingTokens;
    }

    public void setThinkingTokens(Integer thinkingTokens) {
        this.thinkingTokens = thinkingTokens;
    }

    public Integer getCachedTokens() {
        return cachedTokens;
    }

    public void setCachedTokens(Integer cachedTokens) {
        this.cachedTokens = cachedTokens;
    }

    public Integer getMsgMatchTextTokens() {
        return msgMatchTextTokens;
    }

    public void setMsgMatchTextTokens(Integer msgMatchTextTokens) {
        this.msgMatchTextTokens = msgMatchTextTokens;
    }

    public Instant getCreatedTs() {
        return createdTs;
    }

    public void setCreatedTs(Instant createdTs) {
        this.createdTs = createdTs;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getAdditionInfo() {
        return additionInfo;
    }

    public void setAdditionInfo(String additionInfo) {
        this.additionInfo = additionInfo;
    }

    public LocalDate getExecutionDate() {
        return executionDate;
    }

    public void setExecutionDate(LocalDate executionDate) {
        this.executionDate = executionDate;
    }

    public Instant getSentDate() {
        return sentDate;
    }

    public void setSentDate(Instant sentDate) {
        this.sentDate = sentDate;
    }

    public String getRunDate() {
        return runDate;
    }

    public void setRunDate(String runDate) {
        this.runDate = runDate;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public Instant getGeminiRequestStartTime() {
        return geminiRequestStartTime;
    }

    public void setGeminiRequestStartTime(Instant geminiRequestStartTime) {
        this.geminiRequestStartTime = geminiRequestStartTime;
    }

    public Instant getGeminiRequestEndTime() {
        return geminiRequestEndTime;
    }

    public void setGeminiRequestEndTime(Instant geminiRequestEndTime) {
        this.geminiRequestEndTime = geminiRequestEndTime;
    }

    public String getRerunProcessId() {
        return rerunProcessId;
    }

    public void setRerunProcessId(String rerunProcessId) {
        this.rerunProcessId = rerunProcessId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PipelineRecordAuditRow)) {
            return false;
        }
        PipelineRecordAuditRow other = (PipelineRecordAuditRow) obj;
        return Objects.equals(processId, other.processId) && Objects.equals(triggerType, other.triggerType)
                && Objects.equals(evalTestId, other.evalTestId) && Objects.equals(pipelineExecId, other.pipelineExecId)
                && Objects.equals(recordId, other.recordId) && Objects.equals(stageName, other.stageName)
                && Objects.equals(msgInputFileNm, other.msgInputFileNm)
                && Objects.equals(msgInputFilePath, other.msgInputFilePath)
                && Objects.equals(msgOutputFilePath, other.msgOutputFilePath)
                && Objects.equals(msgOutputFileType, other.msgOutputFileType)
                && Objects.equals(msgOutputFileNm, other.msgOutputFileNm)
                && Objects.equals(status, other.status) && Objects.equals(returnCode, other.returnCode)
                && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(evaluatedRulesDtls, other.evaluatedRulesDtls)
                && Objects.equals(evaluatedRulesCnt, other.evaluatedRulesCnt)
                && Objects.equals(detectedRulesDtls, other.detectedRulesDtls)
                && Objects.equals(detectedRulesCnt, other.detectedRulesCnt)
                && Objects.equals(sysPromptEvalRulesTokens, other.sysPromptEvalRulesTokens)
                && Objects.equals(inputTokens, other.inputTokens) && Objects.equals(outputTokens, other.outputTokens)
                && Objects.equals(thinkingTokens, other.thinkingTokens)
                && Objects.equals(cachedTokens, other.cachedTokens)
                && Objects.equals(msgMatchTextTokens, other.msgMatchTextTokens)
                && Objects.equals(createdTs, other.createdTs) && Objects.equals(createdBy, other.createdBy)
                && Objects.equals(additionInfo, other.additionInfo)
                && Objects.equals(executionDate, other.executionDate) && Objects.equals(sentDate, other.sentDate)
                && Objects.equals(runDate, other.runDate) && Objects.equals(sourceName, other.sourceName)
                && Objects.equals(geminiRequestStartTime, other.geminiRequestStartTime)
                && Objects.equals(geminiRequestEndTime, other.geminiRequestEndTime)
                && Objects.equals(rerunProcessId, other.rerunProcessId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, triggerType, evalTestId, pipelineExecId, recordId, stageName, msgInputFileNm,
                msgInputFilePath, msgOutputFilePath, msgOutputFileType, msgOutputFileNm, status, returnCode,
                errorMessage, evaluatedRulesDtls, evaluatedRulesCnt, detectedRulesDtls, detectedRulesCnt,
                sysPromptEvalRulesTokens, inputTokens, outputTokens, thinkingTokens, cachedTokens,
                msgMatchTextTokens, createdTs, createdBy, additionInfo, executionDate, sentDate, runDate,
                sourceName, geminiRequestStartTime, geminiRequestEndTime, rerunProcessId);
    }

    @Override
    public String toString() {
        return "PipelineRecordAuditRow[processId=" + processId + ", triggerType=" + triggerType
                + ", evalTestId=" + evalTestId + ", pipelineExecId=" + pipelineExecId + ", recordId=" + recordId
                + ", stageName=" + stageName + ", msgInputFileNm=" + msgInputFileNm
                + ", msgInputFilePath=" + msgInputFilePath + ", msgOutputFilePath=" + msgOutputFilePath
                + ", msgOutputFileType=" + msgOutputFileType + ", msgOutputFileNm=" + msgOutputFileNm
                + ", status=" + status + ", returnCode=" + returnCode + ", errorMessage=" + errorMessage
                + ", evaluatedRulesDtls=" + evaluatedRulesDtls + ", evaluatedRulesCnt=" + evaluatedRulesCnt
                + ", detectedRulesDtls=" + detectedRulesDtls + ", detectedRulesCnt=" + detectedRulesCnt
                + ", sysPromptEvalRulesTokens=" + sysPromptEvalRulesTokens + ", inputTokens=" + inputTokens
                + ", outputTokens=" + outputTokens + ", thinkingTokens=" + thinkingTokens
                + ", cachedTokens=" + cachedTokens + ", msgMatchTextTokens=" + msgMatchTextTokens
                + ", createdTs=" + createdTs + ", createdBy=" + createdBy + ", additionInfo=" + additionInfo
                + ", executionDate=" + executionDate + ", sentDate=" + sentDate + ", runDate=" + runDate
                + ", sourceName=" + sourceName + ", geminiRequestStartTime=" + geminiRequestStartTime
                + ", geminiRequestEndTime=" + geminiRequestEndTime + ", rerunProcessId=" + rerunProcessId + "]";
    }
}
