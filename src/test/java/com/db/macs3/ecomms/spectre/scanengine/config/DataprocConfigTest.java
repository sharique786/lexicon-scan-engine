package com.db.macs3.ecomms.spectre.scanengine.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("DataprocConfig")
class DataprocConfigTest {

    // The real sample YAML shape --config_file_path points to in production.
    private static final String SAMPLE_YAML = """
            project_id: db-dev-tugr-mp-spectre
            region: europe-west4
            cluster_name: spectre-dataproc-v3
            workflow_timeout_seconds: 43200

            spring:
              config:
                activate:
                  on-profile: dev

            spectre:
              engine:
                hyperscan:
                  hdb-gcs-bucket: db-dev-euwe3-gcs-109910-3-spectre-policy-config-dev
                  hdb-gcs-prefix: policy_test

                messages:
                  msg-gcs-bucket: db-dev-euwe3-gcs-109910-3-spectre-source-data-dev
                  msg-gcs-prefix: coreapp-trans

                bigquery:
                  bq-project: db-dev-tugr-mp-spectre
                  bq-dataset: spectre_audit
                  bq-view-name: vw_src_msg_lexicon_decision_mapping
                  bq-feature-master: db-dev-tugr-mp-spectre.spectre_audit.feature-master
                  bq-language-feature-dec: db-dev-tugr-mp-spectre.spectre_audit.language-feature-decision
                  bq-output-feature-hit-summary: db-dev-tugr-mp-spectre.spectre_audit.feature-hit-summary
                  bq-output-hit-summary: db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-summary
                  bq-output-hit-restricted: db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-restricted
                  bq-output-hit-unrestricted: db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-unrestricted
                  bq-output-stage-audit: db-dev-tugr-mp-spectre.spectre_audit.pipeline-stage-audit
                  bq-output-record-audit: db-dev-tugr-mp-spectre.spectre_audit.pipeline-record-audit
            """;

    @Test
    @DisplayName("parses the real config_file_path YAML shape end to end")
    void parsesSampleYaml() throws Exception {
        DataprocConfig config = DataprocConfig.parseYaml(
                new ByteArrayInputStream(SAMPLE_YAML.getBytes(StandardCharsets.UTF_8)));

        assertThat(config.projectId()).isEqualTo("db-dev-tugr-mp-spectre");
        assertThat(config.region()).isEqualTo("europe-west4");
        assertThat(config.clusterName()).isEqualTo("spectre-dataproc-v3");
        assertThat(config.workflowTimeoutSeconds()).isEqualTo(43200L);

        assertThat(config.hyperscan().hdbGcsBucket()).isEqualTo("db-dev-euwe3-gcs-109910-3-spectre-policy-config-dev");
        assertThat(config.hyperscan().hdbGcsPrefix()).isEqualTo("policy_test");

        assertThat(config.messages().msgGcsBucket()).isEqualTo("db-dev-euwe3-gcs-109910-3-spectre-source-data-dev");
        assertThat(config.messages().msgGcsPrefix()).isEqualTo("coreapp-trans");

        BqTableConfig bq = config.bigquery();
        assertThat(bq.bqProject()).isEqualTo("db-dev-tugr-mp-spectre");
        assertThat(bq.bqDataset()).isEqualTo("spectre_audit");
        assertThat(bq.bqViewName()).isEqualTo("vw_src_msg_lexicon_decision_mapping");
        assertThat(bq.fullyQualifiedViewName())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.vw_src_msg_lexicon_decision_mapping");
        assertThat(bq.bqFeatureMaster()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.feature-master");
        assertThat(bq.bqLanguageFeatureDec())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.language-feature-decision");
        assertThat(bq.bqOutputFeatureHitSummary())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.feature-hit-summary");
        assertThat(bq.bqOutputHitSummary()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-summary");
        assertThat(bq.bqOutputHitRestricted())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-restricted");
        assertThat(bq.bqOutputHitUnrestricted())
                .isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.lexicon-hit-unrestricted");
        assertThat(bq.bqOutputStageAudit()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.pipeline-stage-audit");
        assertThat(bq.bqOutputRecordAudit()).isEqualTo("db-dev-tugr-mp-spectre.spectre_audit.pipeline-record-audit");
    }

    @Test
    @DisplayName("ignores unrecognised top-level blocks (e.g. spring:) rather than failing")
    void ignoresUnknownBlocks() throws Exception {
        String yamlWithExtraBlock = SAMPLE_YAML + "\nunexpected_future_field: some-value\n";
        DataprocConfig config = DataprocConfig.parseYaml(
                new ByteArrayInputStream(yamlWithExtraBlock.getBytes(StandardCharsets.UTF_8)));
        assertThat(config.projectId()).isEqualTo("db-dev-tugr-mp-spectre");
    }

    @Test
    @DisplayName("throws on genuinely malformed YAML syntax rather than silently mishandling it")
    void throwsOnMalformedYamlSyntax() {
        String brokenYaml = "project_id: db-dev-tugr-mp-spectre\n  bad_indent: [unterminated";
        assertThatThrownBy(() -> DataprocConfig.parseYaml(
                new ByteArrayInputStream(brokenYaml.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("a document missing the whole spectre: block parses (top-level fields still bind) rather than throwing at parse time")
    void missingSpectreBlockParsesWithNullSpectre() {
        String yamlWithoutSpectre = """
                project_id: db-dev-tugr-mp-spectre
                region: europe-west4
                cluster_name: spectre-dataproc-v3
                workflow_timeout_seconds: 43200
                """;
        DataprocConfig config = DataprocConfig.parseYaml(
                new ByteArrayInputStream(yamlWithoutSpectre.getBytes(StandardCharsets.UTF_8)));
        assertThat(config.projectId()).isEqualTo("db-dev-tugr-mp-spectre");
        assertThat(config.spectre()).isNull();
        assertThatThrownBy(config::hyperscan).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("equals/hashCode/toString are value-based, including for every nested config class")
    void equalsHashCodeAndToStringAreValueBased() throws Exception {
        DataprocConfig first = DataprocConfig.parseYaml(
                new ByteArrayInputStream(SAMPLE_YAML.getBytes(StandardCharsets.UTF_8)));
        DataprocConfig second = DataprocConfig.parseYaml(
                new ByteArrayInputStream(SAMPLE_YAML.getBytes(StandardCharsets.UTF_8)));

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second).isEqualTo(first);
        assertThat(first).isNotEqualTo(null);
        assertThat(first).isNotEqualTo("not a DataprocConfig");
        assertThat(first.toString()).contains("db-dev-tugr-mp-spectre").contains("europe-west4");

        assertThat(first.spectre()).isEqualTo(second.spectre()).hasSameHashCodeAs(second.spectre());
        assertThat(first.spectre().toString()).contains("EngineConfig");

        assertThat(first.spectre().engine()).isEqualTo(second.spectre().engine())
                .hasSameHashCodeAs(second.spectre().engine());
        assertThat(first.spectre().engine().toString()).contains("hyperscan").contains("messages").contains("bigquery");

        assertThat(first.hyperscan()).isEqualTo(second.hyperscan()).hasSameHashCodeAs(second.hyperscan());
        assertThat(first.hyperscan()).isNotEqualTo(null).isNotEqualTo("not a HyperscanGcsConfig");
        assertThat(first.hyperscan().toString()).contains("policy_test");

        assertThat(first.messages()).isEqualTo(second.messages()).hasSameHashCodeAs(second.messages());
        assertThat(first.messages()).isNotEqualTo(null).isNotEqualTo("not a MessagesGcsConfig");
        assertThat(first.messages().toString()).contains("coreapp-trans");
    }

    @Test
    @DisplayName("differing hyperscan bucket/prefix values are not equal")
    void differingHyperscanConfigNotEqual() {
        DataprocConfig.HyperscanGcsConfig a = new DataprocConfig.HyperscanGcsConfig("bucket-a", "policy_test");
        DataprocConfig.HyperscanGcsConfig b = new DataprocConfig.HyperscanGcsConfig("bucket-b", "policy_test");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    @DisplayName("differing messages bucket/prefix values are not equal")
    void differingMessagesConfigNotEqual() {
        DataprocConfig.MessagesGcsConfig a = new DataprocConfig.MessagesGcsConfig("bucket-a", "coreapp-trans");
        DataprocConfig.MessagesGcsConfig b = new DataprocConfig.MessagesGcsConfig("bucket-a", "a-different-prefix");
        assertThat(a).isNotEqualTo(b);
    }
}
