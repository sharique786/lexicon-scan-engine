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
import org.apache.spark.api.java.function.MapFunction;
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
 *   <li>Read the BigQuery view in one filtered query covering every {@code dataset_details} entry
 *       ({@link FeatureDecisionViewReader}) and compute its distinct {@code message_id} count. <b>If
 *       that count is zero</b> (no rows matched this run's filter), {@link #runPipeline} returns
 *       immediately — no Hyperscan resolution, no AVRO read, no output table other than
 *       {@code pipeline_stage_audit} is touched, and the {@code SUCCESS} row below is written with
 *       {@code input_record_count = 0}/{@code output_record_count = 0}. This is treated as a normal,
 *       successful "nothing to do" completion, not a failure</li>
 *   <li>Otherwise: resolve the Hyperscan base path — one GCS listing call ({@link HyperscanPathResolver})</li>
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
        writeStageAudit(sparkSession, tableConfig, runtimeArgs, dataprocConfig.stageName(), jobStart, null,
                BqColumns.JobStatus.IN_PROGRESS, null, null, null, null);

        RunStats stats = new RunStats();
        try {
            runPipeline(sparkSession, runtimeArgs, tableConfig, dataprocConfig, stats);
            Instant jobEnd = Instant.now();
            log.info("Job completed successfully in {}ms", Duration.between(jobStart, jobEnd).toMillis());
            writeStageAudit(sparkSession, tableConfig, runtimeArgs, dataprocConfig.stageName(), jobStart, jobEnd,
                    BqColumns.JobStatus.SUCCESS, null, null, stats.inputRecordCount, stats.outputRecordCount);
        } catch (Exception e) {
            Instant jobEnd = Instant.now();
            log.error("Job failed after {}ms: {}", Duration.between(jobStart, jobEnd).toMillis(), e.getMessage(), e);
            writeStageAudit(sparkSession, tableConfig, runtimeArgs, dataprocConfig.stageName(), jobStart, jobEnd,
                    BqColumns.JobStatus.FAILED, 0, e.toString(), stats.inputRecordCount, stats.outputRecordCount);
            throw e;
        }
    }

    // Package-private rather than private: ScanEngineJobRunnerTest calls this directly (with
    // FeatureDecisionViewReader.readFiltered mocked static) to exercise the empty-view early return
    // without needing a live BigQuery connection.
    void runPipeline(SparkSession spark, RuntimeArgs runtimeArgs, BqTableConfig tableConfig,
                     DataprocConfig dataprocConfig, RunStats stats) {

        // Read the view FIRST, before anything else (Hyperscan resolution, AVRO reads, any output
        // table besides pipeline_stage_audit) — an empty view means there is nothing at all to
        // process this run, and the early return just below is what makes that a graceful no-op
        // completion rather than a failure. See that early return for the full rationale.
        Dataset<Row> viewRows = FeatureDecisionViewReader.readFiltered(spark, tableConfig, runtimeArgs).cache();
        Dataset<Row> relevantMessageIds = viewRows.select(BqColumns.View.MESSAGE_ID).distinct().cache();
        long viewDistinctMessageCount = relevantMessageIds.count();
        log.info("Total unique message count from BigQuery view: {}", viewDistinctMessageCount);
        stats.inputRecordCount = (int) viewDistinctMessageCount;

        if (viewDistinctMessageCount == 0) {
            // No BigQuery view rows for this run's filter (process_id/feature_partition_value/
            // policy_engine_id/dataset_partition(s)) — previously this fell through into
            // MessageAvroReader (which can throw NoAvroFilesFoundException for the same, correlated
            // reason: no data this run) and/or into writing empty Datasets to every output table,
            // either of which turned "nothing to do" into a FAILED job. Per requirement: an empty
            // view is not a failure — write nothing except pipeline_stage_audit (via the SUCCESS row
            // `run()` writes right after this method returns, using the 0 values set below) and stop.
            stats.outputRecordCount = 0;
            log.warn("BigQuery view returned no rows for this run (process_id={}, feature_partition_value={}, "
                            + "policy_engine_id={}) — nothing to process; only pipeline_stage_audit will be "
                            + "written (SUCCESS, input_record_count=0, output_record_count=0).",
                    runtimeArgs.processId(), runtimeArgs.featurePartitionValue(), runtimeArgs.policyEngineId());
            return;
        }

        // Resolve the Hyperscan base path — one GCS listing call total for this whole run.
        DataprocConfig.HyperscanGcsConfig hyperscanConfig = dataprocConfig.hyperscan();
        String hyperscanBasePath = runStage("resolve Hyperscan base path", () -> HyperscanPathResolver.resolveBasePath(
                hyperscanConfig.hdbGcsBucket(), hyperscanConfig.hdbGcsPrefix(), runtimeArgs.policyEngineId(),
                gcsClient::listImmediateChildDirectories));
        log.info("Hyperscan base path for this run: {}", hyperscanBasePath);

        // Resolve every DISTINCT feature the (non-empty) view referenced to its .zip bundle path.
        // collectAsList is the stage's real Spark action.
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

        // Read + union AVRO messages (restricted to the view's message_id set), then aggregate
        // the view by message_id, join, and attach output-facing columns. These are lazy
        // transformations: the logged duration reflects DAG construction, not execution.
        DataprocConfig.MessagesGcsConfig messagesConfig = dataprocConfig.messages();
        AvroReadResult avroReadResult = runStage("read AVRO messages + join with view", () -> {
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

            long avroDistinctMessageCount = messages.select(BqColumns.View.MESSAGE_ID).distinct().count();

            if (log.isDebugEnabled()) {
                log.info("Total unique message count read from AVRO across all dataset(s): {}", avroDistinctMessageCount);
                logMessageIdDiffIfDebug("BigQuery view", relevantMessageIds.as(Encoders.STRING()), viewDistinctMessageCount,
                        "AVRO read", messages.select(BqColumns.View.MESSAGE_ID).distinct().as(Encoders.STRING()),
                        avroDistinctMessageCount);
            }

            Dataset<Row> groupedView = FeatureDecisionViewReader.groupByMessageId(viewRows);
            Dataset<Row> joinedRows = messages.join(groupedView, BqColumns.View.MESSAGE_ID)
                    .withColumn(JoinedRowColumns.PIPELINE_EXEC_ID_FOR_OUTPUT, functions.lit(runtimeArgs.pipelineExecId()))
                    .withColumn(JoinedRowColumns.CREATED_BY_FOR_OUTPUT, functions.lit(properties.getCreatedBy()))
                    .withColumn(JoinedRowColumns.DATASET_PARTITION_VALUE_FOR_OUTPUT,
                            functions.col(JoinedRowColumns.DATASET_PARTITION_VALUE));
            return new AvroReadResult(joinedRows, messages, avroDistinctMessageCount);
        });

        Dataset<Row> joined = avroReadResult.joined();

        // mapPartitions — the only place Hyperscan bundles are loaded. Still lazy: scanning happens
        // when .count() below (or, failing that, the writes in step 7) trigger it.
        Dataset<MessageProcessingResult> results = joined.mapPartitions(
                new PartitionProcessor(broadcastFeatureToZipPath,
                        messagesConfig.maxAttachmentLimit(), properties.getMaxCachedDatabasesPerPartition()),
                Encoders.kryo(MessageProcessingResult.class)
        ).cache();

        if (log.isDebugEnabled()) {
            long finalMessageCount = results.count();
            log.info("Final message count before writing output: {}", finalMessageCount);
            logMessageIdDiffIfDebug("AVRO read",
                    avroReadResult.avroMessages().select(BqColumns.View.MESSAGE_ID).distinct().as(Encoders.STRING()),
                    avroReadResult.avroDistinctMessageCount(),
                    "final (pre-write)",
                    results.map((MapFunction<MessageProcessingResult, String>) MessageProcessingResult::getMessageId,
                            Encoders.STRING()),
                    finalMessageCount);
        }

        // Split and write — each write triggers its own Spark action, executing step 6 above
        // (already materialized/cached by the .count() call, so these reuse it rather than rescanning).
        runStageVoid("scan messages + write outputs",
                () -> writeOutputs(tableConfig, runtimeArgs, messagesConfig, dataprocConfig.stageName(), results, stats));
    }

    /**
     * The lazy transformation "read AVRO messages + join with view" stage needs to hand back to its
     * caller, alongside the joined {@code Dataset} itself: the per-dataset-unioned AVRO messages
     * ({@link #logMessageIdDiffIfDebug} needs their message_id set again once the final message count
     * is known, after this stage has already returned — cheap to re-derive since each dataset's own
     * piece is already cached inside {@code MessageAvroReader.readDataset}) and their distinct
     * message count (logged, and compared against the final message count).
     */
    private record AvroReadResult(Dataset<Row> joined, Dataset<Row> avroMessages, long avroDistinctMessageCount) {
    }

    /**
     * Mutable, driver-only holder for the {@code input_record_count}/{@code output_record_count}
     * values {@link #runPipeline} computes partway through — {@link #run} needs them for the
     * SUCCESS/FAILED {@code pipeline_stage_audit} row it writes AFTER {@link #runPipeline} returns
     * (or throws), so a plain return value would lose them on the failure path. Never serialized to
     * a Spark task — read/written only on the driver. Package-private (class and fields) so
     * {@code ScanEngineJobRunnerTest} can construct one and read the values {@link #runPipeline} sets.
     */
    static final class RunStats {
        Integer inputRecordCount;
        Integer outputRecordCount;
    }

    /**
     * Debug-only diagnostic: when {@code countA != countB} AND debug logging is enabled, computes and
     * logs the actual message ids present in one side but not the other (via {@code Dataset.except},
     * not collected unless both conditions hold — a mismatch is expected to be a rare edge case, not
     * the common path, so this never runs against the common, matching-counts case, and never
     * collects anything to the driver when debug logging is off).
     */
    private void logMessageIdDiffIfDebug(String labelA, Dataset<String> idsA, long countA,
                                         String labelB, Dataset<String> idsB, long countB) {
        if (countA == countB || !log.isDebugEnabled()) {
            return;
        }
        List<String> onlyInA = idsA.except(idsB).collectAsList();
        List<String> onlyInB = idsB.except(idsA).collectAsList();
        log.debug("Unique message count mismatch: {} has {} unique message id(s), {} has {} unique message id(s); "
                        + "message id(s) only in {}: {}; message id(s) only in {}: {}",
                labelA, countA, labelB, countB, labelA, onlyInA, labelB, onlyInB);
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
                              DataprocConfig.MessagesGcsConfig messagesConfig, String stageName,
                              Dataset<MessageProcessingResult> results, RunStats stats) {
        Dataset<Row> summaryRows = results.mapPartitions(
                new SummaryRowMapper(), Encoders.row(OutputTableWriter.LEXICON_HIT_SUMMARY_SCHEMA)).cache();
        OutputTableWriter.writeLexiconHitSummary(tableConfig, summaryRows);

        long outputRecordCount = summaryRows.count();
        stats.outputRecordCount = (int) outputRecordCount;
        log.info("Total message count saved to lexicon-hit-summary: {}", outputRecordCount);

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
                new PipelineRecordAuditRowMapper(runtimeArgs, stageName, properties.getCreatedBy(), today),
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
     * @param errorCount        written to the INTEGER {@code error_count} column (0 when null)
     * @param inputRecordCount  unique message count from the BigQuery view (null on the
     *                          {@code IN_PROGRESS} row, where it isn't known yet)
     * @param outputRecordCount number of messages saved to {@code lexicon-hit-summary} (null on the
     *                          {@code IN_PROGRESS} row, where it isn't known yet)
     */
    private void writeStageAudit(SparkSession spark, BqTableConfig tableConfig, RuntimeArgs runtimeArgs,
                                 String stageName, Instant startTime, Instant endTime, String status,
                                 Integer errorCount, String errorMessage, Integer inputRecordCount,
                                 Integer outputRecordCount) {
        // composerDagName/composerDagPath/dprocScriptName/dprocScriptPath are NOT NULL in the table
        // (see PipelineStageAuditRow) but neither RuntimeArgs nor DataprocConfig carries them, so a
        // placeholder is written.
        PipelineStageAuditRow row = new PipelineStageAuditRow(
                runtimeArgs.processId(), runtimeArgs.triggerType(), null, runtimeArgs.pipelineExecId(),
                stageName, STAGE_AUDIT_UNKNOWN_STRING, STAGE_AUDIT_UNKNOWN_STRING,
                STAGE_AUDIT_UNKNOWN_STRING, STAGE_AUDIT_UNKNOWN_STRING, null,
                startTime, endTime, status, 0, 0, inputRecordCount, outputRecordCount,
                errorCount == null ? 0 : errorCount, errorMessage, null, null,
                LocalDate.now(ZoneOffset.UTC), null, null, null);
        OutputTableWriter.writePipelineStageAudit(spark, tableConfig, row);
    }
}
