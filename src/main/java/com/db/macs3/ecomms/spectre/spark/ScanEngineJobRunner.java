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
 * Driver-side orchestrator for one scan job run. Spring-managed and constructed only on
 * the driver — see {@link LexiconScanEngineApplication} for why the executor-side classes it
 * calls into ({@link PartitionProcessor} and everything inside it) are plain, Spring-independent Java.
 *
 * <h2>Pipeline, in order ({@link #runPipeline})</h2>
 * <ol>
 *   <li>Parse {@link RuntimeArgs} from the 7 {@code --key=value} Dataproc arguments, then read the
 *       {@link DataprocConfig} YAML that {@code --config_file_path} points to (BigQuery identifiers and
 *       the Hyperscan/message GCS locations)</li>
 *   <li>Write the {@code IN_PROGRESS} {@code pipeline_stage_audit} row</li>
 *   <li>Resolve the Hyperscan base path — one GCS listing call ({@link HyperscanPathResolver})</li>
 *   <li>Read the BigQuery view in one filtered query covering every {@code dataset_details} entry
 *       ({@link FeatureDecisionViewReader}); it stays a distributed {@code Dataset}</li>
 *   <li>Collect the (small) set of DISTINCT lexicon features referenced, resolve each to its
 *       {@code .zip} bundle path, and broadcast that one feature → path map</li>
 *   <li>Read the AVRO messages of every {@code dataset_details} entry, restricted to the view's
 *       {@code message_id} set ({@link MessageAvroReader}), and union them</li>
 *   <li>Aggregate the view to one row per message, join it to the messages, and add the columns
 *       {@code PartitionProcessor} needs that neither source has ({@code pipeline_exec_id},
 *       {@code created_by}, the output-facing dataset partition value)</li>
 *   <li>{@code mapPartitions} via {@link PartitionProcessor} — the only place Hyperscan bundles are
 *       loaded, one {@link com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader} per partition</li>
 *   <li>Split the per-message results into per-table {@code Dataset}s and write each, write the
 *       restricted-detail CSV mirror, and write {@code pipeline_record_audit}</li>
 *   <li>Write the {@code SUCCESS}/{@code FAILED} {@code pipeline_stage_audit} row</li>
 * </ol>
 *
 * <h2>Driver load</h2>
 * <p>The driver only ever holds the small string-only feature → path map and the distinct
 * feature-name list used to build it (bounded by the number of distinct lexicon features, not by
 * message count). Every message-scale dataset stays a Spark {@code Dataset} from creation to write;
 * the driver never calls {@code .collect()} on any of them.
 *
 * <p>The {@code SparkSession}/{@code JavaSparkContext} are injected; {@link SparkSessionConfig} builds
 * them and applies the job-specific Spark runtime configuration.
 */
@Service
public class ScanEngineJobRunner {

    private static final Logger log = LoggerFactory.getLogger(ScanEngineJobRunner.class);

    private final GcsClient gcsClient;
    private final ScanEngineProperties properties;
    private final SparkSession sparkSession;
    private final JavaSparkContext javaSparkContext;

    /**
     * {@code sparkSession}/{@code javaSparkContext} are injected rather than
     * built here — see {@link SparkSessionConfig}, which owns both beans
     * (including the job-specific Spark runtime config this class used to
     * apply itself) so this class only orchestrates the pipeline against an
     * already-configured session.
     */
    public ScanEngineJobRunner(GcsClient gcsClient, ScanEngineProperties properties,
                               SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.gcsClient = gcsClient;
        this.properties = properties;
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }

    /**
     * @param args the 7 {@code --key=value} Dataproc arguments Composer supplies — see
     *             {@link RuntimeArgs}. {@code --config_file_path} is a GCS path to a
     *             {@link DataprocConfig} YAML file, read here to obtain the {@link BqTableConfig}
     *             plus the Hyperscan/message GCS locations.
     */
    public void run(String[] args) throws Exception {
        log.info("Stage [parse arguments/config]: starting");
        Instant stageStart = Instant.now();
        RuntimeArgs runtimeArgs;
        DataprocConfig dataprocConfig;
        try {
            // Nothing can be written to pipeline_stage_audit yet (that needs tableConfig, which
            // comes from the config file read here), so a failure is logged with stage context and rethrown.
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
        log.info("Parsed input runtime arguments: {}", runtimeArgs);
        log.info("Loaded Dataproc config from {}: {}", runtimeArgs.configFilePath(), dataprocConfig);
        BqTableConfig tableConfig = dataprocConfig.bigquery();

        Instant jobStart = Instant.now();
        log.info("Job starting: processId={}, pipelineExecId={}, policyEngineId={}",
                runtimeArgs.processId(), runtimeArgs.pipelineExecId(), runtimeArgs.policyEngineId());
        writeStageAudit(sparkSession, tableConfig, runtimeArgs, jobStart, null, BqColumns.JobStatus.IN_PROGRESS, null, null);

        try {
            runPipeline(sparkSession, runtimeArgs, tableConfig, dataprocConfig);
            Instant jobEnd = Instant.now();
            log.info("Job completed successfully in {}ms", Duration.between(jobStart, jobEnd).toMillis());
            writeStageAudit(sparkSession, tableConfig, runtimeArgs, jobStart, jobEnd, BqColumns.JobStatus.SUCCESS, null, null);
        } catch (Exception e) {
            Instant jobEnd = Instant.now();
            log.error("Job failed after {}ms: {}", Duration.between(jobStart, jobEnd).toMillis(), e.getMessage(), e);
            writeStageAudit(sparkSession, tableConfig, runtimeArgs, jobStart, jobEnd, BqColumns.JobStatus.FAILED, 0, e.toString());
            throw e;
        }
    }

    private void runPipeline(SparkSession spark, RuntimeArgs runtimeArgs, BqTableConfig tableConfig,
                             DataprocConfig dataprocConfig) {

        // 1. Resolve the Hyperscan base path — one GCS listing call total for this whole run.
        DataprocConfig.HyperscanGcsConfig hyperscanConfig = dataprocConfig.hyperscan();
        String hyperscanBasePath = runStage("resolve Hyperscan base path", () -> HyperscanPathResolver.resolveBasePath(
                hyperscanConfig.hdbGcsBucket(), hyperscanConfig.hdbGcsPrefix(), runtimeArgs.policyEngineId(),
                gcsClient::listImmediateChildDirectories));
        log.info("Hyperscan base path for this run: {}", hyperscanBasePath);

        // 2 + 3. Read the view (one query covering every dataset_details entry), then resolve every
        // DISTINCT feature referenced to its .zip bundle path. collectAsList is the stage's real Spark
        // action (.cache() alone does not materialise); the list is bounded by feature count, not
        // message count, so collecting it to the driver is safe.
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
            log.info("Resolved Hyperscan zip bundle path(s) for this run: {}", zipPaths);
            return zipPaths;
        });
        // One broadcast: the Compile Service writes one zip per feature (the .hdb and the term-metadata
        // JSON together), so HyperscanBundleLoader needs a single feature -> path map. Broadcast via
        // JavaSparkContext (the raw Scala SparkContext needs an implicit ClassTag Java cannot supply).
        Broadcast<Map<String, String>> broadcastFeatureToZipPath = javaSparkContext.broadcast(featureToZipPath);

        // 4 + 5. Read + union AVRO messages (restricted to the view's message_id set), then aggregate
        // the view by message_id, join, and attach output-facing columns. These are lazy
        // transformations: the logged duration reflects DAG construction, not execution.
        DataprocConfig.MessagesGcsConfig messagesConfig = dataprocConfig.messages();
        Dataset<Row> joined = runStage("read AVRO messages + join with view", () -> {
            Dataset<Row> relevantMessageIds = viewRows.select(BqColumns.View.MESSAGE_ID).distinct();
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

        // 6. mapPartitions — the only place Hyperscan bundles are loaded. Still lazy: scanning happens
        // when the writes in step 7 trigger it.
        Dataset<MessageProcessingResult> results = joined.mapPartitions(
                new PartitionProcessor(broadcastFeatureToZipPath,
                        properties.getMaxAttachmentSizeBytes(), properties.getMaxCachedDatabasesPerPartition()),
                Encoders.kryo(MessageProcessingResult.class)
        ).cache();

        // 7. Split and write — each write triggers its own Spark action, executing steps 4-6.
        runStageVoid("scan messages + write outputs",
                () -> writeOutputs(tableConfig, runtimeArgs, messagesConfig, results));
    }

    /**
     * Runs one named pipeline stage, logging start, completion (with elapsed time) and, on failure,
     * the stage name and elapsed time before rethrowing unchanged. {@link #run} still owns overall
     * job success/failure and the {@code pipeline_stage_audit} rows.
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

    // Each output table's Dataset<Row> is built with Dataset<MessageProcessingResult>.mapPartitions(
    // MapPartitionsFunction, Encoders.row(schema)) against a dedicated mapper class in this package
    // (SummaryRowMapper, DetailRowMapper, FeatureHitSummaryRowMapper, PipelineRecordAuditRowMapper).
    // A mapper class holds only serializable fields, so it cannot accidentally capture this
    // (non-serializable) runner the way a lambda referencing an instance field would.
    private void writeOutputs(BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                              DataprocConfig.MessagesGcsConfig messagesConfig, Dataset<MessageProcessingResult> results) {
        Dataset<Row> summaryRows = results.mapPartitions(
                new SummaryRowMapper(), Encoders.row(OutputTableWriter.LEXICON_HIT_SUMMARY_SCHEMA));
        OutputTableWriter.writeLexiconHitSummary(tableConfig, summaryRows);

        Dataset<Row> restrictedDetailRows = results.mapPartitions(
                new DetailRowMapper(true), Encoders.row(OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA));
        OutputTableWriter.writeLexiconHitDetail(tableConfig, restrictedDetailRows, true);
        writeRestrictedCsvMirror(runtimeArgs, messagesConfig, restrictedDetailRows);

        Dataset<Row> unrestrictedDetailRows = results.mapPartitions(
                new DetailRowMapper(false), Encoders.row(OutputTableWriter.LEXICON_HIT_DETAIL_SCHEMA));
        OutputTableWriter.writeLexiconHitDetail(tableConfig, unrestrictedDetailRows, false);

        Dataset<Row> featureHitRows = results.mapPartitions(
                new FeatureHitSummaryRowMapper(), Encoders.row(OutputTableWriter.FEATURE_HIT_SUMMARY_SCHEMA));
        OutputTableWriter.writeFeatureHitSummary(tableConfig, featureHitRows);

        // Every record — success and failure alike — gets a row here, each with its own SUCCESS/FAILED
        // status. Only the identity/status/error columns plus sentDate/runDate/sourceName (copied from the
        // AVRO message) are populated; every other column belongs to stages this job does not run — see
        // PipelineRecordAuditRowMapper.
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        Dataset<Row> recordAuditRows = results.mapPartitions(
                new PipelineRecordAuditRowMapper(runtimeArgs, properties.getStageName(), properties.getCreatedBy(), today),
                Encoders.row(OutputTableWriter.PIPELINE_RECORD_AUDIT_SCHEMA));
        // De-duplicated on the table's natural key (record_id/stage_name/execution_date/pipeline_exec_id):
        // the same message_id can appear more than once in `results`, and the mapper would otherwise
        // emit one audit row per occurrence.
        Dataset<Row> dedupedRecordAuditRows = recordAuditRows.dropDuplicates(
                BqColumns.PipelineRecordAudit.RECORD_ID, BqColumns.PipelineRecordAudit.STAGE_NAME,
                BqColumns.PipelineRecordAudit.EXECUTION_DATE, BqColumns.PipelineRecordAudit.PIPELINE_EXEC_ID);
        if (!dedupedRecordAuditRows.isEmpty()) {
            OutputTableWriter.writePipelineRecordAudit(tableConfig, dedupedRecordAuditRows);
        }
    }

    /**
     * Mirrors the restricted detail rows to a single CSV file on GCS, at the same {@code restricted/}
     * location {@link MessageAvroReader} reads the restricted AVRO messages from for this run's
     * (first) dataset, with a {@code csv} subfolder appended — see
     * {@link MessageAvroReader#restrictedPath}.
     */
    private void writeRestrictedCsvMirror(RuntimeArgs runtimeArgs, DataprocConfig.MessagesGcsConfig messagesConfig,
                                          Dataset<Row> restrictedDetailRows) {
        String datasetId = runtimeArgs.datasetDetails().getFirst().datasetId();
        String csvPath = MessageAvroReader.restrictedPath(
                messagesConfig.msgGcsBucket(), messagesConfig.msgGcsPrefix(), datasetId) + "csv";
        // CSV cannot hold nested array/struct columns, so evaluated_lexicons is written as a JSON string.
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
     * Placeholder for the NOT NULL Composer DAG / Dataproc script columns of {@code pipeline_stage_audit}.
     */
    private static final String STAGE_AUDIT_UNKNOWN_STRING = "N/A";

    /**
     * @param errorCount written to the INTEGER {@code error_count} column (0 when null)
     */
    private void writeStageAudit(SparkSession spark, BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                                 Instant startTime, Instant endTime, String status,
                                 Integer errorCount, String errorMessage) {
        // composerDagName/composerDagPath/dprocScriptName/dprocScriptPath are NOT NULL in the table
        // (see PipelineStageAuditRow) but neither RuntimeArgs nor DataprocConfig carries them, so a
        // placeholder is written.
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
