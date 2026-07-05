package com.db.macs3.ecomms.spectre.reader;

import com.db.macs3.ecomms.spectre.model.JobConfig;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableInfo;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("BigQueryViewReader Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryViewReaderTest {

    @Mock
    private BigQuery mockBigQuery;

    private BigQueryViewReader reader;
    private JobConfig config;

    @BeforeEach
    void setUp() {
        reader = new BigQueryViewReader(mockBigQuery);
        config = new JobConfig();
        config.setBqProject("my-project");
        config.setBqDataset("working_dataset");
        config.setViewName("v_lexicon_scan_engine_input");
        config.getInputTables().languageFeatureDecision = "spectre-audit.language-feature-decision";
        config.getInputTables().featureMaster = "spectre-audit.feature-master";
    }

    // ── SQL structure ────────────────────────────────────────────────────────

    @Test @Order(1)
    @DisplayName("buildViewSql contains CREATE OR REPLACE VIEW with correct reference")
    void viewSql_correctViewRef() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).contains("CREATE OR REPLACE VIEW");
        assertThat(sql).contains("my-project.working_dataset.v_lexicon_scan_engine_input");
    }

    @Test @Order(2)
    @DisplayName("buildViewSql references both source tables from config")
    void viewSql_referencesSourceTables() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).contains("spectre-audit.language-feature-decision");
        assertThat(sql).contains("spectre-audit.feature-master");
    }

    @Test @Order(3)
    @DisplayName("buildViewSql unnests LFD.features and filters direct lexicon type")
    void viewSql_lfdDirectLexicon() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).containsIgnoringCase("UNNEST(lfd.features)");
        assertThat(sql).contains("feat.type = 'lexicon'");
    }

    @Test @Order(4)
    @DisplayName("buildViewSql unnests LFD composite sub_feature filtered to type='lexicon'")
    void viewSql_lfdCompositeSubFeature() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).containsIgnoringCase("UNNEST(feat.sub_feature)");
        assertThat(sql).contains("feat.type = 'composite'");
        assertThat(sql).contains("sf.type = 'lexicon'");
    }

    @Test @Order(5)
    @DisplayName("buildViewSql resolves FM direct lexicon feature_definition")
    void viewSql_fmDirectLexicon() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).contains("fm.feature_type = 'lexicon'");
        assertThat(sql).contains("fm.feature_definition");
    }

    @Test @Order(6)
    @DisplayName("buildViewSql resolves FM Composite sub_feature.definition filtered to lexicon")
    void viewSql_fmCompositeSubFeature() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).containsIgnoringCase("UNNEST(fm.sub_feature)");
        assertThat(sql).contains("fm.feature_type = 'Composite'");
        assertThat(sql).contains("sf.definition");
    }

    @Test @Order(7)
    @DisplayName("buildViewSql joins on process_id = policy_engine_id and lexicon_name match")
    void viewSql_joinConditions() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).contains("lfd.process_id");
        assertThat(sql).contains("fm.policy_engine_id");
        assertThat(sql).contains("lfd.lexicon_name");
        assertThat(sql).contains("fm.lexicon_name");
    }

    @Test @Order(8)
    @DisplayName("buildViewSql contains UNION ALL for both LFD and FM combined CTEs")
    void viewSql_unionAll() {
        String sql = reader.buildViewSql(config);
        long unionCount = sql.lines().filter(l -> l.trim().equalsIgnoreCase("UNION ALL")).count();
        assertThat(unionCount).isEqualTo(2); // one for lfd_flat, one for fm_all
    }

    @Test @Order(9)
    @DisplayName("buildViewSql retains parent feature identity columns on every emitted row")
    void viewSql_retainsParentIdentity() {
        String sql = reader.buildViewSql(config);
        assertThat(sql).contains("feat.id AS feature_id");
        assertThat(sql).contains("feat.name AS feature_name");
        assertThat(sql).contains("feat.operator AS feature_operator");
        assertThat(sql).contains("feat.is_noise_reduction AS is_noise_reduction_raw");
    }

    // ── View creation lifecycle ──────────────────────────────────────────────

    @Test @Order(20)
    @DisplayName("createOrReplaceView updates an existing view")
    void createOrReplace_updatesExisting() {
        when(mockBigQuery.update(any(TableInfo.class))).thenReturn(null);
        assertThatCode(() -> reader.createOrReplaceView(config)).doesNotThrowAnyException();
        verify(mockBigQuery, times(1)).update(any(TableInfo.class));
        verify(mockBigQuery, never()).create(any(TableInfo.class));
    }

    @Test @Order(21)
    @DisplayName("createOrReplaceView creates the view when BQ returns 404 on update")
    void createOrReplace_createsWhenNotFound() {
        when(mockBigQuery.update(any(TableInfo.class))).thenThrow(new BigQueryException(404, "Not found"));
        when(mockBigQuery.create(any(TableInfo.class))).thenReturn(null);
        assertThatCode(() -> reader.createOrReplaceView(config)).doesNotThrowAnyException();
        verify(mockBigQuery, times(1)).create(any(TableInfo.class));
    }

    @Test @Order(22)
    @DisplayName("createOrReplaceView propagates non-404 BQ exceptions")
    void createOrReplace_propagatesOtherErrors() {
        when(mockBigQuery.update(any(TableInfo.class))).thenThrow(new BigQueryException(500, "Internal error"));
        assertThatThrownBy(() -> reader.createOrReplaceView(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to create/update BQ view");
    }
}
