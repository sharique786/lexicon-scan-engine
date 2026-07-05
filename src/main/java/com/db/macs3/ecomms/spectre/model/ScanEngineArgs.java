package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;

/**
 * Typed container for the runtime arguments passed to the Lexicon Scan Engine
 * from the Airflow DAG via the Dataproc job submission command line.
 *
 * <p>Per the platform convention, table/view names are NOT passed here — they
 * live in a {@link JobConfig} JSON file on GCS referenced by
 * {@link #getConfigGcsPath()}. This class carries only per-execution identity
 * and audit metadata that changes on every run.
 *
 * <h2>Runtime arguments</h2>
 * <ul>
 *   <li>{@code --processId}       — process_id shared with NLF Engine / feature-master.policy_engine_id</li>
 *   <li>{@code --pipelineExecId}  — pipeline execution instance identifier</li>
 *   <li>{@code --policyEngineId}  — policy engine instance identifier</li>
 *   <li>{@code --triggerType}     — free-form trigger label, e.g. "policy-alert-test" or "policy-alert-live"</li>
 *   <li>{@code --evalTestId}      — evaluation/test run identifier (blank for live runs)</li>
 *   <li>{@code --runDate}         — run date, e.g. "20260713"</li>
 *   <li>{@code --configGcsPath}   — gs:// path to the {@link JobConfig} JSON file</li>
 *   <li>{@code --compsrDagName}   — Composer/Airflow DAG name, recorded in pipeline_stage_audit</li>
 *   <li>{@code --compsrDagPath}   — Composer/Airflow DAG GCS path</li>
 *   <li>{@code --dprocScriptName} — this Dataproc job's script/jar name, for audit</li>
 *   <li>{@code --dprocScriptPath} — this Dataproc job's script/jar GCS path, for audit</li>
 * </ul>
 */
public class ScanEngineArgs implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STAGE_NAME = "spectre-lexicon-tagging";

    private final String processId;
    private final String pipelineExecId;
    private final String policyEngineId;
    private final String triggerType;
    private final String evalTestId;
    private final String runDate;
    private final String configGcsPath;
    private final String compsrDagName;
    private final String compsrDagPath;
    private final String dprocScriptName;
    private final String dprocScriptPath;

    private ScanEngineArgs(Builder b) {
        this.processId       = b.processId;
        this.pipelineExecId  = b.pipelineExecId;
        this.policyEngineId  = b.policyEngineId;
        this.triggerType     = b.triggerType;
        this.evalTestId      = b.evalTestId;
        this.runDate         = b.runDate;
        this.configGcsPath   = b.configGcsPath;
        this.compsrDagName   = b.compsrDagName;
        this.compsrDagPath   = b.compsrDagPath;
        this.dprocScriptName = b.dprocScriptName;
        this.dprocScriptPath = b.dprocScriptPath;
    }

    /**
     * Parses {@code --key value} command-line arguments.
     *
     * @param args the raw {@code main(String[] args)} array
     * @return a fully populated {@code ScanEngineArgs}
     * @throws IllegalArgumentException if a required argument is missing
     */
    public static ScanEngineArgs parse(String[] args) {
        Builder b = new Builder();
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--processId":        b.processId(args[++i]);        break;
                case "--pipelineExecId":   b.pipelineExecId(args[++i]);   break;
                case "--policyEngineId":   b.policyEngineId(args[++i]);   break;
                case "--triggerType":      b.triggerType(args[++i]);      break;
                case "--evalTestId":       b.evalTestId(args[++i]);       break;
                case "--runDate":          b.runDate(args[++i]);          break;
                case "--configGcsPath":    b.configGcsPath(args[++i]);    break;
                case "--compsrDagName":    b.compsrDagName(args[++i]);    break;
                case "--compsrDagPath":    b.compsrDagPath(args[++i]);    break;
                case "--dprocScriptName":  b.dprocScriptName(args[++i]);  break;
                case "--dprocScriptPath":  b.dprocScriptPath(args[++i]);  break;
                default:
                    // unknown flags ignored — allows pass-through from the DAG
            }
        }
        return b.build();
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String getProcessId()        { return processId; }
    public String getPipelineExecId()   { return pipelineExecId; }
    public String getPolicyEngineId()   { return policyEngineId; }
    public String getTriggerType()      { return triggerType; }
    public String getEvalTestId()       { return evalTestId; }
    public String getRunDate()          { return runDate; }
    public String getConfigGcsPath()    { return configGcsPath; }
    public String getCompsrDagName()    { return compsrDagName; }
    public String getCompsrDagPath()    { return compsrDagPath; }
    public String getDprocScriptName()  { return dprocScriptName; }
    public String getDprocScriptPath()  { return dprocScriptPath; }

    @Override
    public String toString() {
        return "ScanEngineArgs{processId='" + processId + "', pipelineExecId='" + pipelineExecId +
               "', policyEngineId='" + policyEngineId + "', triggerType='" + triggerType +
               "', evalTestId='" + evalTestId + "', runDate='" + runDate +
               "', configGcsPath='" + configGcsPath + "'}";
    }

    public static class Builder {
        private String processId, pipelineExecId, policyEngineId, triggerType,
                       evalTestId, runDate, configGcsPath,
                       compsrDagName, compsrDagPath, dprocScriptName, dprocScriptPath;

        public Builder processId(String v)       { this.processId = v;       return this; }
        public Builder pipelineExecId(String v)  { this.pipelineExecId = v;  return this; }
        public Builder policyEngineId(String v)  { this.policyEngineId = v;  return this; }
        public Builder triggerType(String v)     { this.triggerType = v;     return this; }
        public Builder evalTestId(String v)      { this.evalTestId = v;      return this; }
        public Builder runDate(String v)         { this.runDate = v;         return this; }
        public Builder configGcsPath(String v)   { this.configGcsPath = v;   return this; }
        public Builder compsrDagName(String v)   { this.compsrDagName = v;   return this; }
        public Builder compsrDagPath(String v)   { this.compsrDagPath = v;   return this; }
        public Builder dprocScriptName(String v) { this.dprocScriptName = v; return this; }
        public Builder dprocScriptPath(String v) { this.dprocScriptPath = v; return this; }

        public ScanEngineArgs build() {
            requireNonBlank(processId, "processId");
            requireNonBlank(pipelineExecId, "pipelineExecId");
            requireNonBlank(policyEngineId, "policyEngineId");
            requireNonBlank(triggerType, "triggerType");
            requireNonBlank(runDate, "runDate");
            requireNonBlank(configGcsPath, "configGcsPath");
            // evalTestId, compsr*/dproc* are optional (blank for some run types)
            return new ScanEngineArgs(this);
        }

        private void requireNonBlank(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Required argument missing or blank: --" + name);
            }
        }
    }
}
