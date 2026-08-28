package com.db.macs3.ecomms.spectre.scanengine.spark;

import com.db.macs3.ecomms.spectre.scanengine.avro.MessageAvroReader;
import com.db.macs3.ecomms.spectre.scanengine.bq.FeatureDecisionViewReader;
import com.db.macs3.ecomms.spectre.scanengine.bq.OutputTableWriter;
import com.db.macs3.ecomms.spectre.scanengine.config.BqTableConfig;
import com.db.macs3.ecomms.spectre.scanengine.config.RuntimeArgs;
import com.db.macs3.ecomms.spectre.scanengine.config.ScanEngineProperties;
import com.db.macs3.ecomms.spectre.scanengine.constants.BqColumns;
import com.db.macs3.ecomms.spectre.scanengine.gcs.GcsClient;
import com.db.macs3.ecomms.spectre.scanengine.gcs.HyperscanPathResolver;
import com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.scanengine.model.output.PipelineRecordAuditRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.PipelineStageAuditRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
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
 * method. See {@link ScanEngineApplication} class Javadoc for why this
 * class (constructed and run entirely on the DRIVER) is the appropriate
 * place for Spring dependency injection, while the executor-side classes it
 * calls into ({@link PartitionProcessor} and everything inside it) remain
 * plain, Spring-independent Java.
 *
 * <h2>Wiring order</h2>
 * <ol>
 *   <li>Parse {@link RuntimeArgs} + {@link BqTableConfig} (requirement 4.b)</li>
 *   <li>Write the {@code IN_PROGRESS} {@code pipeline_stage_audit} row (requirement 3.a)</li>
 *   <li>Resolve the Hyperscan base path — ONE GCS listing call (see {@code HyperscanPathResolver})</li>
 *   <li>Read + union the view across every {@code dataset_details} entry, filtered
 *       (requirement 1.c) — stays a distributed {@code Dataset}</li>
 *   <li>Collect the (small) set of DISTINCT features referenced, resolve each to its
 *       {@code .zip} bundle path, and broadcast that small map — see class Javadoc "Driver load"</li>
 *   <li>Read + union AVRO messages across every {@code dataset_details} entry
 *       (requirement 1.c/8.d), restricted by the view's own {@code message_id} set
 *       (requirement 1.e)</li>
 *   <li>Aggregate the view by {@code message_id}, join against messages, attach the
 *       few extra columns {@code PartitionProcessor} needs that are not in either
 *       source (pipeline_exec_id, created_by, output-facing dataset_partition_value)</li>
 *   <li>{@code mapPartitions} via {@link PartitionProcessor} — the only place Hyperscan
 *       databases are loaded, one {@link com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanBundleLoader}
 *       per partition</li>
 *   <li>Split the per-message results into per-table {@code Dataset}s and write each
 *       (requirement 3.b); write the {@code lexicon-hit-restricted} CSV mirror
 *       (requirement 3.g); write {@code pipeline_record_audit} for any per-message
 *       failures (requirement 3, "Other errors")</li>
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
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox (no live Spark cluster/GCP credentials here) — see
 * {@code GcsClient} class Javadoc, which applies to this whole orchestration
 * layer. Should be exercised against the integration test setup (requirement
 * 4.d) before first production use.
 */
@Service
public class ScanEngineJobRunner {

    private static final Logger log = LoggerFactory.getLogger(ScanEngineJobRunner.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final GcsClient gcsClient;
    private final ScanEngineProperties properties;

    public ScanEngineJobRunner(GcsClient gcsClient, ScanEngineProperties properties) {
        this.gcsClient = gcsClient;
        this.properties = properties;
    }

    /**
     * @param args {@code [0]} = path to the {@link RuntimeArgs} JSON,
     *              {@code [1]} = path to the {@link BqTableConfig} JSON —
     *              both GCS paths, per requirement 4.b
     */
    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Usage: ScanEngineApplication <runtime-args-gcs-path> <bq-table-config-gcs-path>");
        }

        RuntimeArgs runtimeArgs = JSON.readValue(gcsClient.readTextFile(args[0]), RuntimeArgs.class);
        BqTableConfig tableConfig = BqTableConfig.parse(
                new ByteArrayInputStream(gcsClient.readTextFile(args[1]).getBytes(StandardCharsets.UTF_8)));

        SparkSession spark = SparkSession.builder().appName("lexicon-scan-engine").getOrCreate();

        Instant jobStart = Instant.now();
        writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, null, BqColumns.JobStatus.IN_PROGRESS, null, null);

        try {
            runPipeline(spark, runtimeArgs, tableConfig);
            writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, Instant.now(),
                    BqColumns.JobStatus.SUCCESS, null, null);
        } catch (Exception e) {
            log.error("Job failed: {}", e.getMessage(), e);
            writeStageAudit(spark, tableConfig, runtimeArgs, jobStart, Instant.now(),
                    BqColumns.JobStatus.FAILED, "0", e.toString());
            throw e;
        }
    }

    private void runPipeline(SparkSession spark, RuntimeArgs runtimeArgs, BqTableConfig tableConfig) {

        // 1. Resolve the Hyperscan base path — one GCS listing call total for this whole run.
        String hyperscanBasePath = HyperscanPathResolver.resolveBasePath(
                properties.getEnvironmentBucket(), runtimeArgs.policyEngineId(),
                gcsClient::listImmediateChildDirectories);

        // 2. Read + union the view across every dataset_details entry.
        List<Dataset<Row>> perDatasetView = new ArrayList<>();
        for (RuntimeArgs.DatasetDetail dd : runtimeArgs.datasetDetails()) {
            perDatasetView.add(FeatureDecisionViewReader.readFiltered(
                    spark, tableConfig, dd.datasetPartitionValue(), runtimeArgs.featurePartitionValue(),
                    runtimeArgs.processId()));
        }
        Dataset<Row> viewRows = FeatureDecisionViewReader.unionAll(spark, perDatasetView).cache();

        // 3. Resolve every DISTINCT feature referenced to its .zip bundle path, and broadcast the
        //    resulting small (feature -> path) map — see class Javadoc "Driver load". The
        //    distinct-feature list itself is bounded by feature count, not message count,
        //    so collecting it to the driver is safe.
        List<String> distinctFeatureDefJson = viewRows.select(BqColumns.View.FEATURE_DEFINITION)
                .distinct().as(Encoders.STRING()).collectAsList();
        Set<String> distinctFeatures = distinctFeatureDefJson.stream()
                .map(FeatureDefinition::parse)
                .map(fd -> fd.body().feature())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, String> featureToZipPath = new HashMap<>();
        for (String feature : distinctFeatures) {
            featureToZipPath.put(feature, HyperscanPathResolver.buildZipPath(hyperscanBasePath, feature));
        }
        // Broadcast via JavaSparkContext (not the raw Scala SparkContext, which requires an
        // implicit ClassTag that Java code cannot supply naturally) — the standard Java-side
        // way to create a Broadcast. ONE broadcast now — the Compile Service writes one zip
        // bundle per feature (containing both the .hdb and the term-metadata JSON), so
        // HyperscanBundleLoader needs only one feature -> path map — see that class Javadoc.
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Broadcast<Map<String, String>> broadcastFeatureToZipPath = javaSparkContext.broadcast(featureToZipPath);

        // 4. Read + union AVRO messages, restricted to the view's own message_id set.
        Dataset<Row> relevantMessageIds = viewRows.select(BqColumns.View.MESSAGE_ID).distinct();
        String messageBucket = properties.resolveMessageBucket(runtimeArgs);
        List<Dataset<Row>> perDatasetMessages = new ArrayList<>();
        for (RuntimeArgs.DatasetDetail dd : runtimeArgs.datasetDetails()) {
            perDatasetMessages.add(MessageAvroReader.readDataset(
                    spark, gcsClient, messageBucket, dd.datasetId(), relevantMessageIds));
        }
        Dataset<Row> messages = perDatasetMessages.get(0);
        for (int i = 1; i < perDatasetMessages.size(); i++) {
            messages = messages.unionByName(perDatasetMessages.get(i), true);
        }

        // 5. Aggregate the view by message_id, join, attach output-facing columns.
        Dataset<Row> groupedView = FeatureDecisionViewReader.groupByMessageId(viewRows);
        Dataset<Row> joined = messages.join(groupedView, BqColumns.View.MESSAGE_ID)
                .withColumn("pipeline_exec_id_for_output", functions.lit(runtimeArgs.pipelineExecId()))
                .withColumn("created_by_for_output", functions.lit(properties.getCreatedBy()))
                .withColumn("dataset_partition_value_for_output", functions.col("dataset_id"));

        // 6. mapPartitions — the only place Hyperscan databases are loaded.
        Dataset<MessageProcessingResult> results = joined.mapPartitions(
                new PartitionProcessor(broadcastFeatureToZipPath,
                        properties.getMaxAttachmentSizeBytes(), properties.getMaxCachedDatabasesPerPartition()),
                Encoders.kryo(MessageProcessingResult.class)
        ).cache();

        // 7. Split and write.
        writeOutputs(spark, tableConfig, runtimeArgs, results);
    }

    private void writeOutputs(SparkSession spark, BqTableConfig tableConfig,
                               RuntimeArgs runtimeArgs, Dataset<MessageProcessingResult> results) {
        // Explicit FilterFunction typing: a bare lambda here is ambiguous between Dataset's Java
        // API (FilterFunction<T>) and its Scala API (Function1<T, Object>), both of which are
        // structurally compatible with a T -> boolean lambda.
        Dataset<MessageProcessingResult> successes =
                results.filter((FilterFunction<MessageProcessingResult>) r -> !r.isError());
        Dataset<MessageProcessingResult> failures =
                results.filter((FilterFunction<MessageProcessingResult>) MessageProcessingResult::isError);

        JavaRDD<Row> summaryRows = successes.javaRDD().map(r -> OutputTableWriter.toRow(r.summaryRow()));
        OutputTableWriter.writeLexiconHitSummary(spark, tableConfig, summaryRows);

        JavaRDD<Row> restrictedDetailRows = successes.javaRDD()
                .filter(r -> r.restricted() && r.detailRow() != null)
                .map(r -> OutputTableWriter.toRow(r.detailRow()));
        OutputTableWriter.writeLexiconHitDetail(spark, tableConfig, restrictedDetailRows, true);
        writeRestrictedCsvMirror(spark, runtimeArgs, restrictedDetailRows);

        JavaRDD<Row> unrestrictedDetailRows = successes.javaRDD()
                .filter(r -> !r.restricted() && r.detailRow() != null)
                .map(r -> OutputTableWriter.toRow(r.detailRow()));
        OutputTableWriter.writeLexiconHitDetail(spark, tableConfig, unrestrictedDetailRows, false);

        JavaRDD<Row> featureHitRows = successes.javaRDD().map(r -> OutputTableWriter.toRow(r.featureHitSummaryRow()));
        OutputTableWriter.writeFeatureHitSummary(spark, tableConfig, featureHitRows);

        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        JavaRDD<Row> recordAuditRows = failures.javaRDD().map(r -> OutputTableWriter.toRow(new PipelineRecordAuditRow(
                runtimeArgs.processId(), runtimeArgs.triggerType(), runtimeArgs.pipelineExecId(),
                properties.getStageName(), r.messageId(), BqColumns.RecordStatus.FAILED, 1,
                r.errorMessage(), today, properties.getCreatedBy(), Instant.now())));
        if (!recordAuditRows.isEmpty()) {
            OutputTableWriter.writePipelineRecordAudit(spark, tableConfig, recordAuditRows);
        }
    }

    /** Requirement 3.g: mirror the restricted detail rows to a CSV file on GCS. */
    private void writeRestrictedCsvMirror(SparkSession spark, RuntimeArgs runtimeArgs, JavaRDD<Row> restrictedDetailRows) {
        String csvPath = "gs://" + properties.getEnvironmentBucket() + "/" + runtimeArgs.policyEngineId()
                + "/" + runtimeArgs.processId() + "/restricted/" + runtimeArgs.pipelineExecId() + ".csv";
        Dataset<Row> ds = spark.createDataFrame(restrictedDetailRows, OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA);
        // Spark's own CSV writer cannot represent nested array/struct columns directly — the
        // evaluated_lexicons column is flattened to its JSON string form specifically for this
        // CSV mirror, since CSV has no native nested-value representation.
        ds.withColumn("evaluated_lexicons", functions.to_json(functions.col("evaluated_lexicons")))
                .coalesce(1) // one CSV file at this path, matching requirement 3.g's single-file target
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(csvPath);
    }

    private void writeStageAudit(SparkSession spark, BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                                  Instant startTime, Instant endTime, String status,
                                  String errorCount, String errorMessage) {
        PipelineStageAuditRow row = new PipelineStageAuditRow(
                runtimeArgs.processId(), runtimeArgs.triggerType(), runtimeArgs.pipelineExecId(),
                properties.getStageName(), null, null, null, null,
                startTime, endTime, status, errorCount, errorMessage, null,
                LocalDate.now(ZoneOffset.UTC));
        OutputTableWriter.writePipelineStageAudit(spark, tableConfig, row);
    }
}
