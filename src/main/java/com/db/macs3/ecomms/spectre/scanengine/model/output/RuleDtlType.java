package com.db.macs3.ecomms.spectre.scanengine.model.output;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * One entry of {@code pipeline_record_audit.evaluated_rules_dtls}/
 * {@code detected_rules_dtls} — identifies a single rule this record was
 * evaluated against (or, for the detected list, actually hit).
 *
 * <p>Constructor parameter order ({@code ruleVersion, ruleName, ruleId})
 * deliberately does NOT match the delivered schema's struct field order
 * ({@code rule_id, rule_name, rule_version}) — kept verbatim from the
 * delivered class shape. {@code OutputTableWriter} builds the nested
 * {@code Row} in SCHEMA field order, not constructor-argument order; do not
 * assume the two agree when touching that mapping.
 */
public class RuleDtlType implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Integer ruleVersion;
    private String ruleName;
    private String ruleId;

    public RuleDtlType(Integer ruleVersion, String ruleName, String ruleId) {
        this.ruleVersion = ruleVersion;
        this.ruleName = ruleName;
        this.ruleId = ruleId;
    }

    public Integer getRuleVersion() { return ruleVersion; }
    public void setRuleVersion(Integer ruleVersion) { this.ruleVersion = ruleVersion; }
    public String getRuleName() { return ruleName; }
    public void setRuleName(String ruleName) { this.ruleName = ruleName; }
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RuleDtlType)) {
            return false;
        }
        RuleDtlType other = (RuleDtlType) o;
        return Objects.equals(ruleVersion, other.ruleVersion)
                && Objects.equals(ruleName, other.ruleName)
                && Objects.equals(ruleId, other.ruleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleVersion, ruleName, ruleId);
    }

    @Override
    public String toString() {
        return "RuleDtlType[ruleVersion=" + ruleVersion + ", ruleName=" + ruleName + ", ruleId=" + ruleId + "]";
    }
}
