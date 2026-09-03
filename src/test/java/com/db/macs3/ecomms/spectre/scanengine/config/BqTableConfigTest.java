package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("BqTableConfig")
class BqTableConfigTest {

    private static final String SAMPLE_JSON = "{"
            + "\"bq-project\":\"db-dev-tugr-mp-spectre\","
            + "\"bq-dataset\":\"spectre_audit\","
            + "\"bq-view-name\":\"vw_src_msg_lexicon_decision_mapping\","
            + "\"bq-feature-master\":\"db-dev-tugr-mp-spectre.spectre_audit.feature-master\","
            + "\"bq-language-feature-dec\":\"db-dev-tugr-mp-spectre.spectre_audit.language-feature-decision\","
            + "\"bq-output-feature-hit-summary\":\"db-dev-tugr-mp-spectre.spectre_audit.feature-hit-summary\","
            + "\"bq-output-hit-summary\":\"db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-summary\","
            + "\"bq-output-hit-restricted\":\"db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-restricted\","
            + "\"bq-output-hit-unrestricted\":\"db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-unrestricted\","
            + "\"bq-output-stage-audit\":\"db-dev-tugr-mp-spectre.spectre_audit.pipeline-stage-audit\","
            + "\"bq-output-record-audit\":\"db-dev-tugr-mp-spectre.spectre_audit.pipeline-record-audit\""
            + "}";

    private static BqTableConfig sample() throws Exception {
        return new ObjectMapper().readValue(SAMPLE_JSON, BqTableConfig.class);
    }

    @Test
    @DisplayName("binds every bq-* field to its matching accessor")
    void bindsEveryField() throws Exception {
        BqTableConfig config = sample();

        assertThat(config.bqProject()).isEqualTo("db-dev-tugr-mp-spectre");
        assertThat(config.bqDataset()).isEqualTo("spectre_audit");
        assertThat(config.bqViewName()).isEqualTo("vw_src_msg_lexicon_decision_mapping");
        assertThat(config.bqFeatureMaster()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.feature-master");
        assertThat(config.bqLanguageFeatureDec())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.language-feature-decision");
        assertThat(config.bqOutputFeatureHitSummary())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.feature-hit-summary");
        assertThat(config.bqOutputHitSummary()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-summary");
        assertThat(config.bqOutputHitRestricted())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-restricted");
        assertThat(config.bqOutputHitUnrestricted())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-unrestricted");
        assertThat(config.bqOutputStageAudit()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.pipeline-stage-audit");
        assertThat(config.bqOutputRecordAudit())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.pipeline-record-audit");
    }

    @Test
    @DisplayName("fullyQualifiedViewName concatenates bq-project.bq-dataset.bq-view-name")
    void buildsFullyQualifiedViewName() throws Exception {
        assertThat(sample().fullyQualifiedViewName())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.vw_src_msg_lexicon_decision_mapping");
    }

    @Test
    @DisplayName("equals/hashCode are value-based across two independently-parsed instances")
    void equalsAndHashCodeAreValueBased() throws Exception {
        BqTableConfig first = sample();
        BqTableConfig second = sample();

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
        assertThat(first).isNotEqualTo(null);
        assertThat(first).isNotEqualTo("not a BqTableConfig");
        assertThat(first).isEqualTo(first);
    }

    @Test
    @DisplayName("equals returns false when a single field differs")
    void equalsFalseOnSingleFieldDifference() throws Exception {
        BqTableConfig first = sample();
        BqTableConfig differentDataset = new BqTableConfig(first.bqProject(), "a-different-dataset",
                first.bqViewName(), first.bqFeatureMaster(), first.bqLanguageFeatureDec(),
                first.bqOutputFeatureHitSummary(), first.bqOutputHitSummary(), first.bqOutputHitRestricted(),
                first.bqOutputHitUnrestricted(), first.bqOutputStageAudit(), first.bqOutputRecordAudit());

        assertThat(first).isNotEqualTo(differentDataset);
    }

    @Test
    @DisplayName("toString includes every field's value, for debugging/audit logging")
    void toStringIncludesEveryField() throws Exception {
        String text = sample().toString();
        assertThat(text)
                .contains("db-dev-tugr-mp-spectre")
                .contains("spectre_audit")
                .contains("vw_src_msg_lexicon_decision_mapping")
                .contains("feature-master")
                .contains("language-feature-decision")
                .contains("feature-hit-summary")
                .contains("lexicon-hit-summary")
                .contains("lexicon-hit-restricted")
                .contains("lexicon-hit-unrestricted")
                .contains("pipeline-stage-audit")
                .contains("pipeline-record-audit");
    }
}
