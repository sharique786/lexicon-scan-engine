package com.db.macs3.ecomms.spectre.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * The {@code model_config_dtls} nested struct of {@code pipeline_stage_audit}
 * — the generative-model configuration a stage ran with, when that stage
 * invokes one as part of its processing (e.g. an LLM-based evaluation
 * stage). Null on {@link PipelineStageAuditRow}s for stages that don't
 * invoke a model at all — this project's own scan stage included.
 *
 * <p>{@link #getTemprature()} is spelled verbatim as delivered by the schema
 * (not "temperature") — kept as-is rather than "corrected", the same
 * convention this project applies to {@code pipeline_exec_id} elsewhere;
 * see {@code BqColumns.PipelineStageAudit.ModelConfigDtls}.
 */
public class ModelConfigDtls implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String modelName;
    private Float temprature;
    private Float topP;
    private Integer thinkingBudget;
    private Integer maxOutputToken;

    /**
     * @param modelName      the invoked model's identifier
     * @param temprature     sic — see class Javadoc
     * @param topP           nucleus-sampling parameter
     * @param thinkingBudget reasoning/thinking token budget, when the model supports one
     * @param maxOutputToken max output tokens allowed for the call
     */
    public ModelConfigDtls(String modelName, Float temprature, Float topP,
                           Integer thinkingBudget, Integer maxOutputToken) {
        this.modelName = modelName;
        this.temprature = temprature;
        this.topP = topP;
        this.thinkingBudget = thinkingBudget;
        this.maxOutputToken = maxOutputToken;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public Float getTemprature() {
        return temprature;
    }

    public void setTemprature(Float temprature) {
        this.temprature = temprature;
    }

    public Float getTopP() {
        return topP;
    }

    public void setTopP(Float topP) {
        this.topP = topP;
    }

    public Integer getThinkingBudget() {
        return thinkingBudget;
    }

    public void setThinkingBudget(Integer thinkingBudget) {
        this.thinkingBudget = thinkingBudget;
    }

    public Integer getMaxOutputToken() {
        return maxOutputToken;
    }

    public void setMaxOutputToken(Integer maxOutputToken) {
        this.maxOutputToken = maxOutputToken;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ModelConfigDtls)) {
            return false;
        }
        ModelConfigDtls other = (ModelConfigDtls) obj;
        return Objects.equals(modelName, other.modelName)
                && Objects.equals(temprature, other.temprature)
                && Objects.equals(topP, other.topP)
                && Objects.equals(thinkingBudget, other.thinkingBudget)
                && Objects.equals(maxOutputToken, other.maxOutputToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, temprature, topP, thinkingBudget, maxOutputToken);
    }

    @Override
    public String toString() {
        return "ModelConfigDtls[modelName=" + modelName + ", temprature=" + temprature + ", topP=" + topP
                + ", thinkingBudget=" + thinkingBudget + ", maxOutputToken=" + maxOutputToken + "]";
    }
}
