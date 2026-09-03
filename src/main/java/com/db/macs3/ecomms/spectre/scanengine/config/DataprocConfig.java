package com.db.macs3.ecomms.spectre.scanengine.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * The YAML file {@code --config_file_path} points to — one per Dataproc
 * workflow-template submission, written by Composer alongside the other 6
 * {@code --key=value} arguments {@link RuntimeArgs} carries directly (see
 * that class's Javadoc for the full argument list and a sample invocation).
 *
 * <p>Sample shape (values illustrative):
 * <pre>
 * project_id: db-dev-tugr-mp-spectre
 * region: europe-west4
 * cluster_name: spectre-dataproc-v3
 * workflow_timeout_seconds: 43200
 *
 * spring:
 *   config:
 *     activate:
 *       on-profile: dev
 *
 * spectre:
 *   engine:
 *     hyperscan:
 *       hdb-gcs-bucket: db-dev-euwe3-gcs-109910-3-spectre-policy-config-dev
 *       hdb-gcs-prefix: policy_test
 *     messages:
 *       msg-gcs-bucket: db-dev-euwe3-gcs-109910-3-spectre-source-data-dev
 *       msg-gcs-prefix: coreapp-trans
 *     bigquery:
 *       bq-project: db-dev-tugr-mp-spectre
 *       bq-dataset: spectre_audit
 *       bq-view-name: vw_src_msg_lexicon_decision_mapping
 *       ... (see {@link BqTableConfig})
 * </pre>
 *
 * <p>This project reads only the {@code spectre.engine.*} subtree (via
 * {@link #hyperscan()}/{@link #messages()}/{@link #bigquery()}) plus
 * {@code project_id}/{@code region}/{@code cluster_name}/
 * {@code workflow_timeout_seconds} at the top level, carried here for
 * completeness though nothing in this engine currently reads them (they
 * describe the Dataproc CLUSTER this job already runs on, supplied
 * separately at {@code gcloud dataproc jobs submit} time — not something
 * this in-cluster job needs to look up about itself). The {@code spring:}
 * block is Composer/orchestrator-facing (a Spring profile marker for a
 * DIFFERENT component in this pipeline, not this job) and is deliberately
 * NOT modelled — the underlying {@link ObjectMapper} is configured to ignore
 * unrecognised properties rather than modelling every field an external,
 * ops-owned YAML file happens to carry.
 *
 * <p>{@code hdb-gcs-prefix}/{@code msg-gcs-prefix} replace what used to be
 * hardcoded path-segment constants ({@code HyperscanPathResolver}'s
 * {@code policy_test}, {@code AvroConstants}' {@code coreapp-trans/}) — both
 * are now environment-supplied, since a folder naming convention baked into
 * this engine's own source was never really a constant, just previously
 * unconfigurable.
 *
 * <p>A plain class rather than a record, matching this project's other
 * pre-existing model classes.
 */
public final class DataprocConfig implements Serializable {

    private final String projectId;
    private final String region;
    private final String clusterName;
    private final Long workflowTimeoutSeconds;
    private final SpectreConfig spectre;

    @JsonCreator
    public DataprocConfig(@JsonProperty("project_id") String projectId,
                           @JsonProperty("region") String region,
                           @JsonProperty("cluster_name") String clusterName,
                           @JsonProperty("workflow_timeout_seconds") Long workflowTimeoutSeconds,
                           @JsonProperty("spectre") SpectreConfig spectre) {
        this.projectId = projectId;
        this.region = region;
        this.clusterName = clusterName;
        this.workflowTimeoutSeconds = workflowTimeoutSeconds;
        this.spectre = spectre;
    }

    public String projectId() { return projectId; }
    public String region() { return region; }
    public String clusterName() { return clusterName; }
    public Long workflowTimeoutSeconds() { return workflowTimeoutSeconds; }
    public SpectreConfig spectre() { return spectre; }

    public HyperscanGcsConfig hyperscan() { return spectre.engine().hyperscan(); }
    public MessagesGcsConfig messages() { return spectre.engine().messages(); }
    public BqTableConfig bigquery() { return spectre.engine().bigquery(); }

    // SafeConstructor rather than SnakeYAML's default Constructor: this file is
    // externally-owned (written by the orchestrator, not this codebase), and SafeConstructor
    // restricts deserialization to plain scalars/maps/lists — never arbitrary Java types via
    // YAML's own "!!" tag syntax — the standard defensive-parsing choice for a YAML document
    // this process doesn't control the contents of.
    private static final Yaml SNAKE_YAML = new Yaml(new SafeConstructor(new LoaderOptions()));
    private static final ObjectMapper BINDING_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /**
     * Parses the YAML file's content — see class Javadoc for where this file
     * lives in production. Two steps, deliberately: SnakeYAML (via
     * {@link Yaml#load(InputStream)}) turns the raw YAML text into a plain
     * {@code Map<String, Object>} tree exactly like a JSON document would
     * parse into with a generic Jackson read, then the existing, already
     * version-pinned {@link ObjectMapper} binds that {@code Map} onto this
     * class's {@code @JsonCreator} constructors via {@code convertValue} —
     * see this class's {@code pom.xml} dependency comment for why this goes
     * through SnakeYAML directly rather than {@code jackson-dataformat-yaml}.
     */
    public static DataprocConfig parseYaml(InputStream yamlStream) {
        Map<String, Object> rawYaml = SNAKE_YAML.load(yamlStream);
        return BINDING_MAPPER.convertValue(rawYaml, DataprocConfig.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataprocConfig)) {
            return false;
        }
        DataprocConfig other = (DataprocConfig) o;
        return Objects.equals(projectId, other.projectId) && Objects.equals(region, other.region)
                && Objects.equals(clusterName, other.clusterName)
                && Objects.equals(workflowTimeoutSeconds, other.workflowTimeoutSeconds)
                && Objects.equals(spectre, other.spectre);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectId, region, clusterName, workflowTimeoutSeconds, spectre);
    }

    @Override
    public String toString() {
        return "DataprocConfig[projectId=" + projectId + ", region=" + region + ", clusterName=" + clusterName
                + ", workflowTimeoutSeconds=" + workflowTimeoutSeconds + ", spectre=" + spectre + "]";
    }

    /** {@code spectre:} — one level of nesting above {@code engine:}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class SpectreConfig implements Serializable {
        private final EngineConfig engine;

        @JsonCreator
        public SpectreConfig(@JsonProperty("engine") EngineConfig engine) {
            this.engine = engine;
        }

        public EngineConfig engine() { return engine; }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof SpectreConfig other && Objects.equals(engine, other.engine));
        }

        @Override
        public int hashCode() { return Objects.hashCode(engine); }

        @Override
        public String toString() { return "SpectreConfig[engine=" + engine + "]"; }
    }

    /** {@code spectre.engine:} — carries the three subsections this job actually reads. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class EngineConfig implements Serializable {
        private final HyperscanGcsConfig hyperscan;
        private final MessagesGcsConfig messages;
        private final BqTableConfig bigquery;

        @JsonCreator
        public EngineConfig(@JsonProperty("hyperscan") HyperscanGcsConfig hyperscan,
                             @JsonProperty("messages") MessagesGcsConfig messages,
                             @JsonProperty("bigquery") BqTableConfig bigquery) {
            this.hyperscan = hyperscan;
            this.messages = messages;
            this.bigquery = bigquery;
        }

        public HyperscanGcsConfig hyperscan() { return hyperscan; }
        public MessagesGcsConfig messages() { return messages; }
        public BqTableConfig bigquery() { return bigquery; }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof EngineConfig other
                    && Objects.equals(hyperscan, other.hyperscan) && Objects.equals(messages, other.messages)
                    && Objects.equals(bigquery, other.bigquery));
        }

        @Override
        public int hashCode() { return Objects.hash(hyperscan, messages, bigquery); }

        @Override
        public String toString() {
            return "EngineConfig[hyperscan=" + hyperscan + ", messages=" + messages + ", bigquery=" + bigquery + "]";
        }
    }

    /**
     * {@code spectre.engine.hyperscan:} — the bucket/prefix
     * {@code HyperscanPathResolver.resolveBasePath} resolves feature zip
     * bundles under (replaces the previous {@code environment-bucket}
     * Spring property and the hardcoded {@code policy_test} constant).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class HyperscanGcsConfig implements Serializable {
        private final String hdbGcsBucket;
        private final String hdbGcsPrefix;

        @JsonCreator
        public HyperscanGcsConfig(@JsonProperty("hdb-gcs-bucket") String hdbGcsBucket,
                                   @JsonProperty("hdb-gcs-prefix") String hdbGcsPrefix) {
            this.hdbGcsBucket = hdbGcsBucket;
            this.hdbGcsPrefix = hdbGcsPrefix;
        }

        public String hdbGcsBucket() { return hdbGcsBucket; }
        public String hdbGcsPrefix() { return hdbGcsPrefix; }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof HyperscanGcsConfig other
                    && Objects.equals(hdbGcsBucket, other.hdbGcsBucket) && Objects.equals(hdbGcsPrefix, other.hdbGcsPrefix));
        }

        @Override
        public int hashCode() { return Objects.hash(hdbGcsBucket, hdbGcsPrefix); }

        @Override
        public String toString() { return "HyperscanGcsConfig[hdbGcsBucket=" + hdbGcsBucket + ", hdbGcsPrefix=" + hdbGcsPrefix + "]"; }
    }

    /**
     * {@code spectre.engine.messages:} — the bucket/prefix
     * {@code MessageAvroReader} reads AVRO message files under (replaces the
     * previous {@code live-message-bucket}/{@code test-message-bucket}
     * Spring properties and the hardcoded {@code coreapp-trans/} constant;
     * see {@code MessageAvroReader} Javadoc for why the live/test split is
     * gone — this YAML supplies exactly one bucket per run now).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class MessagesGcsConfig implements Serializable {
        private final String msgGcsBucket;
        private final String msgGcsPrefix;

        @JsonCreator
        public MessagesGcsConfig(@JsonProperty("msg-gcs-bucket") String msgGcsBucket,
                                  @JsonProperty("msg-gcs-prefix") String msgGcsPrefix) {
            this.msgGcsBucket = msgGcsBucket;
            this.msgGcsPrefix = msgGcsPrefix;
        }

        public String msgGcsBucket() { return msgGcsBucket; }
        public String msgGcsPrefix() { return msgGcsPrefix; }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof MessagesGcsConfig other
                    && Objects.equals(msgGcsBucket, other.msgGcsBucket) && Objects.equals(msgGcsPrefix, other.msgGcsPrefix));
        }

        @Override
        public int hashCode() { return Objects.hash(msgGcsBucket, msgGcsPrefix); }

        @Override
        public String toString() { return "MessagesGcsConfig[msgGcsBucket=" + msgGcsBucket + ", msgGcsPrefix=" + msgGcsPrefix + "]"; }
    }
}
