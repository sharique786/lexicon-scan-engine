package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.LexiconScanEngineApplication;
import com.db.macs3.ecomms.spectre.avro.MessageAvroReader;
import com.db.macs3.ecomms.spectre.bq.FeatureDecisionViewReader;
import com.db.macs3.ecomms.spectre.bq.OutputTableWriter;
import com.db.macs3.ecomms.spectre.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.config.DataprocConfig;
import com.db.macs3.ecomms.spectre.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.config.ScanEngineProperties;
import com.db.macs3.ecomms.spectre.constants.BqColumns;
import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import com.db.macs3.ecomms.spectre.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.model.output.PipelineStageAuditRow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spring-managed driver-side orchestrator for the whole scan job — the
 * injectable counterpart of what used to be a fully static {@code main}
 * method. See {@link LexiconScanEngineApplication} class Javadoc for why this
 * class (constructed and run entirely on the DRIVER) is the appropriate
 * place for Spring dependency injection, while the executor-side classes it
 * calls into ({@link PartitionProcessor} and everything inside it) remain
 * plain, Spring-independent Java.
 *
 * <h2>Wiring order</h2>
 * <ol>
 *   <li>Parse {@link RuntimeArgs} from the 7 {@code --key=value} Dataproc submit
 *       arguments, then read the {@link DataprocConfig} YAML its
 *       {@code --config_file_path} points to for {@link BqTableConfig} and the
 *       Hyperscan/message GCS bucket locations — see {@link RuntimeArgs} class
 *       Javadoc for the full argument list</li>
 *   <li>Write the {@code IN_PROGRESS} {@code pipeline_stage_audit} row</li>
 *   <li>Resolve the Hyperscan base path — ONE GCS listing call (see {@code HyperscanPathResolver})</li>
 *   <li>Read + union the view across every {@code dataset_details} entry, filtered —
 *       stays a distributed {@code Dataset}</li>
 *   <li>Collect the (small) set of DISTINCT features referenced, resolve each to its
 *       {@code .zip} bundle path, and broadcast that small map — see class Javadoc "Driver load"</li>
 *   <li>Read + union AVRO messages across every {@code dataset_details} entry,
 *       restricted by the view's own {@code message_id} set</li>
 *   <li>Aggregate the view by {@code message_id}, join against messages, attach the
 *       few extra columns {@code PartitionProcessor} needs that are not in either
 *       source (pipeline_exec_id, created_by, output-facing dataset_partition_value)</li>
 *   <li>{@code mapPartitions} via {@link PartitionProcessor} — the only place Hyperscan
 *       databases are loaded, one {@link com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader}
 *       per partition</li>
 *   <li>Split the per-message results into per-table {@code Dataset}s and write each;
 *       write the {@code lexicon-hit-restricted} CSV mirror; write
 *       {@code pipeline_record_audit} for any per-message failures</li>
 *   <li>Write the {@code SUCCESS}/{@code FAILED} {@code pipeline_stage_audit} row</li>
 * </ol>
 *
 * <h2>Driver-load discipline</h2>
 * <p>The ONLY things this driver holds/collects at more-than-trivial size are:
 * the small, string-only feature→path map (broadcast, not held per-executor);
 * and the distinct-feature-name list used to build it (bounded by the number
 * of distinct lexicon features a run references, not by message count).
 * Every message-scale dataset (the joined message+view Dataset, the
 * per-message results, every output table) stays a Spark {@code Dataset}
 * from creation to write — this driver never calls {@code .collect()} on any
 * of them.
 */
@Service
public class ScanEngineJobRunner {

    private static final Logger log = LoggerFactory.getLogger(ScanEngineJobRunner.class);

    private final GcsClient gcsClient;
    private final ScanEngineProperties properties;

    public ScanEngineJobRunner(GcsClient gcsClient, ScanEngineProperties properties) {
        this.gcsClient = gcsClient;
        this.properties = properties;
    }

    /**
     * @param args the 7 {@code --key=value} Dataproc submit arguments Composer
     *             now supplies — see {@link RuntimeArgs} class Javadoc for the
     *             full list and a sample invocation. {@code --config_file_path}
     *             is a GCS path to a {@link DataprocConfig} YAML file, read
     *             here to obtain the {@link BqTableConfig} (its
     *             {@code spectre.engine.bigquery} section) plus the Hyperscan/
     *             message GCS bucket locations {@link #runPipeline} needs.
     */
    public void run(String[] args) throws Exception {
        log.info("Stage [parse arguments/config]: starting");
        Instant stageStart = Instant.now();
        RuntimeArgs runtimeArgs;
        DataprocConfig dataprocConfig;
        try {
            // Nothing has been written to pipeline_stage_audit yet at this point (that requires
            // tableConfig, which comes FROM the config file this stage reads) — a failure here
            // would otherwise surface as a raw, context-free stack trace with no indication of
            // which argument or config file was the problem.
            runtimeArgs = RuntimeArgs.parseCliArgs(args);
            dataprocConfig = DataprocConfig.parseYaml(
                    new ByteArrayInputStream(gcsClient.readTextFile(runtimeArgs.configFilePath())
                            .getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            log.error("Stage [parse arguments/config]: failed after {}ms: {}",
                    Duration.between(stageStart, Instant.now()).toMillis(), e.getMessage(), e);
            throw e;
        }
        log.info("Stage [parse arguments/config]: completed in {}ms",
                Duration.between(stageStart, Instant.now()).toMillis());
        BqTableConfig tableConfig = dataprocConfig.bigquery();

        // spark.serializer is a "static" config — only takes effect if set before the
        // SparkContext is actually constructed, via SparkSession.builder().config(...), never
        // via spark.conf().set(...) afterward.
        SparkSession spark = SparkSession.builder()
                .appName("lexicon-scan-engine")
                .config(SparkConfigKeys.SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        applyJobSpecificSparkConf(spark);

        Instant jobStart = Instant.now();
        log.info("Job starting: processId={}, pipelineExecId={}, policyEngineId={}",
                runtimeArgs.processId(), runtimeArgs.pipelineExecId(), runtimeArgs.policyEngineId());
        writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, null, BqColumns.JobStatus.IN_PROGRESS, null, null);

        try {
            runPipeline(spark, runtimeArgs, tableConfig, dataprocConfig);
            Instant jobEnd = Instant.now();
            log.info("Job completed successfully in {}ms", Duration.between(jobStart, jobEnd).toMillis());
            writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, jobEnd, BqColumns.JobStatus.SUCCESS, null, null);
        } catch (Exception e) {
            Instant jobEnd = Instant.now();
            log.error("Job failed after {}ms: {}", Duration.between(jobStart, jobEnd).toMillis(), e.getMessage(), e);
            writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, jobEnd, BqColumns.JobStatus.FAILED, 0, e.toString());
            throw e;
        }
    }

    /**
     * Sets this job's OWN critical performance/skew-handling configs
     * explicitly, rather than trusting the shared Dataproc cluster's
     * {@code spark-defaults.conf} to already suit this specific workload —
     * see README "Performance & scalability" for the full reasoning. On a
     * cluster shared with other tenants, global defaults reflect whatever
     * mix of OTHER jobs has driven them, not this job's own two independent
     * skew sources (a single message's attachment text can be far larger
     * than the median; a single message's applicable-lexicon-feature count
     * can also be far larger than the median, independently of attachment
     * size) — every setting below is a per-{@code SparkSession} RUNTIME
     * config, safe to set here (unlike {@code spark.serializer}, a "static"
     * config that must be set on the builder before {@code getOrCreate()} —
     * see the caller).
     */
    private void applyJobSpecificSparkConf(SparkSession spark) {
        RuntimeConfig conf = spark.conf();

        // AQE + skew-join splitting: on by default since Spark 3.2, but never assumed here —
        // this job's correctness/performance under skew depends on it, so it is asserted
        // explicitly rather than hoped for.
        conf.set(SparkConfigKeys.ADAPTIVE_ENABLED, "true");
        conf.set(SparkConfigKeys.ADAPTIVE_COALESCE_PARTITIONS_ENABLED, "true");
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_ENABLED, "true");
        // Tightened from Spark's own defaults (factor 5, 256MB threshold): a single message
        // with an unusually large attachment can dwarf the median shuffle-partition size by
        // far more than 5x while still being one real, unsplittable row — the default
        // threshold can under-react to exactly this job's specific skew shape. Reasoned
        // defaults, treated as a starting point to monitor and adjust against real production
        // message-size distributions.
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_FACTOR, "3");
        conf.set(SparkConfigKeys.ADAPTIVE_SKEW_JOIN_SKEWED_PARTITION_THRESHOLD_BYTES, "128m");
        conf.set(SparkConfigKeys.ADAPTIVE_ADVISORY_PARTITION_SIZE_BYTES, "64m");

        // spark.sql.shuffle.partitions: Spark's own hardcoded default (200) has no relationship
        // to how many executor cores THIS run actually has on a shared, dynamically-allocated
        // cluster — computed here relative to the driver's own view of available parallelism at
        // job start instead, floored at 200 so a slow dynamic-allocation ramp-up at startup
        // never produces an under-parallelised shuffle. AQE's coalescePartitions still merges
        // this back down post-shuffle as actual data volume allows.
        int defaultParallelism = spark.sparkContext().defaultParallelism();
        conf.set(SparkConfigKeys.SHUFFLE_PARTITIONS, String.valueOf(Math.max(200, defaultParallelism * 3)));

        // Smaller AVRO read-side partitions: the default 128MB max-partition-bytes groups
        // messages into a read partition purely by source-file byte range, with no awareness
        // that one of those bytes might belong to a single message's giant attachment — a
        // smaller ceiling here means fewer OTHER messages get bundled alongside a large one
        // into the same initial partition, reducing (not eliminating — a single giant record
        // is still a single giant record) the odds that one partition's read+decode cost
        // dominates the whole stage's wall-clock time before AQE's post-shuffle rebalancing
        // even has a chance to help.
        conf.set(SparkConfigKeys.FILES_MAX_PARTITION_BYTES, "67108864"); // 64MB
    }

    private void runPipeline(SparkSession spark, RuntimeArgs runtimeArgs, BqTableConfig tableConfig,
                             DataprocConfig dataprocConfig) {

        // 1. Resolve the Hyperscan base path — one GCS listing call total for this whole run.
        DataprocConfig.HyperscanGcsConfig hyperscanConfig = dataprocConfig.hyperscan();
        String hyperscanBasePath = runStage("resolve Hyperscan base path", () -> HyperscanPathResolver.resolveBasePath(
                hyperscanConfig.hdbGcsBucket(), hyperscanConfig.hdbGcsPrefix(), runtimeArgs.policyEngineId(),
                gcsClient::listImmediateChildDirectories));

        // 2 + 3. Read the view (one query covering every dataset_details entry), then resolve every
        // DISTINCT feature referenced to its .zip bundle path and broadcast the resulting small
        // (feature -> path) map — see class Javadoc "Driver load". collectAsList below is the
        // stage's real Spark action (the .cache() alone does not force materialisation); the
        // distinct-feature list itself is bounded by feature count, not message count, so
        // collecting it to the driver is safe.
        Dataset<Row> viewRows = FeatureDecisionViewReader.readFiltered(spark, tableConfig, runtimeArgs).cache();
        Map<String, String> featureToZipPath = runStage("read view + resolve distinct features", () -> {
            List<String> distinctFeatureDefJson = viewRows.select(BqColumns.View.FEATURE_DEFINITION)
                    .distinct().as(Encoders.STRING()).collectAsList();
            Set<String> distinctFeatures = distinctFeatureDefJson.stream()
                    .map(FeatureDefinition::parse)
                    .map(featureDefinition -> featureDefinition.getBody().getLexiconName())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            log.info("Resolved {} distinct lexicon feature(s) for this run", distinctFeatures.size());

            Map<String, String> zipPaths = new HashMap<>();
            for (String feature : distinctFeatures) {
                zipPaths.put(feature, HyperscanPathResolver.buildZipPath(hyperscanBasePath, feature));
            }
            return zipPaths;
        });
        // Broadcast via JavaSparkContext (not the raw Scala SparkContext, which requires an
        // implicit ClassTag that Java code cannot supply naturally) — the standard Java-side
        // way to create a Broadcast. ONE broadcast now — the Compile Service writes one zip
        // bundle per feature (containing both the .hdb and the term-metadata JSON), so
        // HyperscanBundleLoader needs only one feature -> path map — see that class Javadoc.
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Broadcast<Map<String, String>> broadcastFeatureToZipPath = javaSparkContext.broadcast(featureToZipPath);

        // 4 + 5. Read + union AVRO messages (restricted to the view's own message_id set), then
        // aggregate the view by message_id, join, and attach output-facing columns. Both steps stay
        // lazy transformations here — no Spark action runs until mapPartitions/writeOutputs below —
        // so this stage's logged duration reflects DAG construction, not actual read/join execution.
        Dataset<Row> joined = runStage("read AVRO messages + join with view", () -> {
            Dataset<Row> relevantMessageIds = viewRows.select(BqColumns.View.MESSAGE_ID).distinct();
            DataprocConfig.MessagesGcsConfig messagesConfig = dataprocConfig.messages();
            List<Dataset<Row>> perDatasetMessages = new ArrayList<>();
            for (RuntimeArgs.DatasetDetail datasetDetail : runtimeArgs.datasetDetails()) {
                perDatasetMessages.add(MessageAvroReader.readDataset(
                        spark, gcsClient, messagesConfig.msgGcsBucket(), messagesConfig.msgGcsPrefix(),
                        datasetDetail.datasetId(), datasetDetail.datasetPartitionValue(), relevantMessageIds));
            }
            Dataset<Row> messages = perDatasetMessages.getFirst();
            for (int datasetIndex = 1; datasetIndex < perDatasetMessages.size(); datasetIndex++) {
                messages = messages.unionByName(perDatasetMessages.get(datasetIndex), true);
            }

            Dataset<Row> groupedView = FeatureDecisionViewReader.groupByMessageId(viewRows);
            return messages.join(groupedView, BqColumns.View.MESSAGE_ID)
                    .withColumn(JoinedRowColumns.PIPELINE_EXEC_ID_FOR_OUTPUT, functions.lit(runtimeArgs.pipelineExecId()))
                    .withColumn(JoinedRowColumns.CREATED_BY_FOR_OUTPUT, functions.lit(properties.getCreatedBy()))
                    .withColumn(JoinedRowColumns.DATASET_PARTITION_VALUE_FOR_OUTPUT,
                            functions.col(JoinedRowColumns.DATASET_PARTITION_VALUE));
        });

        // 6. mapPartitions — the only place Hyperscan databases are loaded. Still lazy: the actual
        // scan work happens once the writes in step 7 trigger it.
        Dataset<MessageProcessingResult> results = joined.mapPartitions(
                new PartitionProcessor(broadcastFeatureToZipPath,
                        properties.getMaxAttachmentSizeBytes(), properties.getMaxCachedDatabasesPerPartition()),
                Encoders.kryo(MessageProcessingResult.class)
        ).cache();

        // 7. Split and write — this is where steps 4-6's lazy DAG actually executes, as each write
        // below triggers its own Spark action.
        runStageVoid("scan messages + write outputs",
                () -> writeOutputs(tableConfig, runtimeArgs, hyperscanConfig.hdbGcsBucket(), results));
    }

    /**
     * Runs one named pipeline stage, logging its start, completion (with elapsed wall-clock
     * time), and — on failure — the elapsed time up to that failure plus the stage name, before
     * rethrowing unchanged (the outer {@link #run} still owns deciding overall job
     * success/failure and writing {@code pipeline_stage_audit}; this only adds stage-level
     * context to what would otherwise be an undifferentiated exception from deep in the DAG).
     */
    private <T> T runStage(String stageName, java.util.function.Supplier<T> stage) {
        log.info("Stage [{}]: starting", stageName);
        Instant stageStart = Instant.now();
        try {
            T result = stage.get();
            log.info("Stage [{}]: completed in {}ms", stageName, Duration.between(stageStart, Instant.now()).toMillis());
            return result;
        } catch (RuntimeException e) {
            log.error("Stage [{}]: failed after {}ms: {}",
                    stageName, Duration.between(stageStart, Instant.now()).toMillis(), e.getMessage(), e);
            throw e;
        }
    }

    private void runStageVoid(String stageName, Runnable stage) {
        runStage(stageName, () -> {
            stage.run();
            return null;
        });
    }

    // writeOutputs builds each output table's Dataset<Row> via Dataset<MessageProcessingResult>.
    // mapPartitions(MapPartitionsFunction, Encoder<Row>) directly, against a dedicated mapper
    // class in this package — NOT via JavaRDD.map(Function) + spark.createDataFrame(JavaRDD<Row>,
    // StructType), an earlier revision's approach. See OutputTableWriter class Javadoc for why:
    // a JavaRDD.map lambda that references an enclosing instance field (as this method's
    // pipeline_record_audit row-building used to, via `properties.getStageName()`) silently
    // captures the enclosing ScanEngineJobRunner itself, which is not serializable, and fails
    // with "Task not serializable" only once actually run on a cluster — confirmed via a real
    // Dataproc run. A standalone MapPartitionsFunction class holding only genuinely serializable
    // fields (SummaryRowMapper, DetailRowMapper, FeatureHitSummaryRowMapper,
    // PipelineRecordAuditRowMapper — all in this package) avoids that failure mode structurally,
    // not just by careful lambda-capture discipline.
    private void writeOutputs(BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                              String csvMirrorBucket, Dataset<MessageProcessingResult> results) {
        Dataset<Row> summaryRows = results.mapPartitions(
                new SummaryRowMapper(), Encoders.row(OutputTableWriter.LEXICON_HIT_SUMMARY_SCHEMA));
        OutputTableWriter.writeLexiconHitSummary(tableConfig, summaryRows);

        Dataset<Row> restrictedDetailRows = results.mapPartitions(
                new DetailRowMapper(true), Encoders.row(OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA));
        OutputTableWriter.writeLexiconHitDetail(tableConfig, restrictedDetailRows, true);
        writeRestrictedCsvMirror(runtimeArgs, csvMirrorBucket, restrictedDetailRows);

        Dataset<Row> unrestrictedDetailRows = results.mapPartitions(
                new DetailRowMapper(false), Encoders.row(OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA));
        OutputTableWriter.writeLexiconHitDetail(tableConfig, unrestrictedDetailRows, false);

        Dataset<Row> featureHitRows = results.mapPartitions(
                new FeatureHitSummaryRowMapper(), Encoders.row(OutputTableWriter.FEATURE_HIT_SUMMARY_SCHEMA));
        OutputTableWriter.writeFeatureHitSummary(tableConfig, featureHitRows);

        // Every record — success and failure alike — gets a row here, each with its own SUCCESS/FAILED
        // status. Only processId/triggerType/pipelineExecId/recordId/stageName/status/returnCode/
        // errorMessage/executionDate/createdBy/createdTs are populated here — every other field (rule
        // evaluation details, token counts, Gemini request timing, rerun/eval-test linkage) belongs to
        // stages this job doesn't run and has no source data for — see PipelineRecordAuditRowMapper
        // class Javadoc.
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        Dataset<Row> recordAuditRows = results.mapPartitions(
                new PipelineRecordAuditRowMapper(runtimeArgs, properties.getStageName(), properties.getCreatedBy(), today),
                Encoders.row(OutputTableWriter.PIPELINE_RECORD_AUDIT_SCHEMA));
        if (!recordAuditRows.isEmpty()) {
            OutputTableWriter.writePipelineRecordAudit(tableConfig, recordAuditRows);
        }
    }

    /**
     * Mirrors the restricted detail rows to a single CSV file on GCS.
     */
    private void writeRestrictedCsvMirror(RuntimeArgs runtimeArgs, String csvMirrorBucket,
                                          Dataset<Row> restrictedDetailRows) {
        String csvPath = "gs://" + csvMirrorBucket + "/" + runtimeArgs.policyEngineId()
                + "/" + runtimeArgs.processId() + "/restricted/" + runtimeArgs.pipelineExecId() + ".csv";
        // Spark's own CSV writer cannot represent nested array/struct columns directly — the
        // evaluated_lexicons column is flattened to its JSON string form specifically for this
        // CSV mirror, since CSV has no native nested-value representation.
        restrictedDetailRows
                .withColumn(BqColumns.LexiconHitDetail.EVALUATED_LEXICONS,
                        functions.to_json(functions.col(BqColumns.LexiconHitDetail.EVALUATED_LEXICONS)))
                .coalesce(1) // one CSV file at this path
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(csvPath);
    }


    /**
     * Placeholder for the NOT NULL Composer DAG / Dataproc script columns until real values are wired through.
     */
    private static final String STAGE_AUDIT_UNKNOWN_STRING = "N/A";

    /**
     * @param errorCount INTEGER, per the delivered schema (was STRING in an earlier revision)
     */
    private void writeStageAudit(SparkSession spark, BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                                 Instant startTime, Instant endTime, String status,
                                 Integer errorCount, String errorMessage) {
        // composerDagName/composerDagPath/dprocScriptName/dprocScriptPath: neither RuntimeArgs nor
        // DataprocConfig currently carries Composer DAG or Dataproc script name/path values, but the
        // delivered schema marks all four NOT NULL (see PipelineStageAuditRow class Javadoc) — a real
        // BigQuery table enforcing that constraint would reject a null write, so a placeholder is used
        // until real values are wired through.
        PipelineStageAuditRow row = new PipelineStageAuditRow(
                runtimeArgs.processId(), runtimeArgs.triggerType(), null, runtimeArgs.pipelineExecId(),
                properties.getStageName(), STAGE_AUDIT_UNKNOWN_STRING, STAGE_AUDIT_UNKNOWN_STRING,
                STAGE_AUDIT_UNKNOWN_STRING, STAGE_AUDIT_UNKNOWN_STRING, null,
                startTime, endTime, status, 0, 0, 0, 0,
                errorCount == null ? 0 : errorCount, errorMessage, null, null,
                LocalDate.now(ZoneOffset.UTC), null, null, null);
        OutputTableWriter.writePipelineStageAudit(spark, tableConfig, row);
    }
}
