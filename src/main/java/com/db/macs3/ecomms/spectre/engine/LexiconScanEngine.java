package com.db.macs3.ecomms.spectre.engine;

import com.db.macs3.ecomms.spectre.model.*;
import com.db.macs3.ecomms.spectre.reader.BigQueryViewReader;
import com.db.macs3.ecomms.spectre.reader.GcsHyperscanDatabaseLoader;
import com.db.macs3.ecomms.spectre.reader.MessageReader;
import com.db.macs3.ecomms.spectre.writer.BigQueryOutputWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Main orchestrator for the Lexicon Scan Engine Spark job.
 *
 * <h2>Pipeline</h2>
 * <ol>
 *   <li>Read feature decisions from the BigQuery view</li>
 *   <li>Load .hdb bytes + manifests from GCS, broadcast to executors</li>
 *   <li>Read and filter messages (AVRO in production, JSON in tests) via the
 *       injected {@link MessageReader}</li>
 *   <li>Group feature decisions by message_id, join with messages</li>
 *   <li>{@code mapPartitions} with {@link LexiconScanPartitionFunction} — ONE
 *       scan pass produces {@link MessageScanResult}s feeding all 3 output tables</li>
 *   <li>Extract and write {@code lexicon-hit-summary}, {@code lexicon-hit-restricted},
 *       and {@code feature-hit-summary}</li>
 *   <li>Write {@code pipeline_stage_audit} and {@code pipeline_record_audit}</li>
 * </ol>
 */
@Service
public class LexiconScanEngine {

    private static final Logger log = LoggerFactory.getLogger(LexiconScanEngine.class);

    private final BigQueryViewReader         bqViewReader;
    private final GcsHyperscanDatabaseLoader hdbLoader;
    private final MessageReader              messageReader;
    private final BigQueryOutputWriter       bqWriter;

    public LexiconScanEngine(BigQueryViewReader bqViewReader,
                              GcsHyperscanDatabaseLoader hdbLoader,
                              @Qualifier("messageReader") MessageReader messageReader,
                              BigQueryOutputWriter bqWriter) {
        this.bqViewReader  = bqViewReader;
        this.hdbLoader     = hdbLoader;
        this.messageReader = messageReader;
        this.bqWriter      = bqWriter;
    }

    /**
     * Executes the full scan pipeline.
     *
     * @param spark  the active SparkSession
     * @param config parsed job configuration (table names, GCS paths)
     * @param args   parsed runtime arguments (process/pipeline identity, audit metadata)
     */
    public void run(SparkSession spark, JobConfig config, ScanEngineArgs args) {
        Timestamp jobStart = Timestamp.from(Instant.now());
        log.info("Lexicon Scan Engine starting: args={}", args);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        long featureRowCount = 0;
        String errorMessage = null;
        String jobStatus = PipelineStageAuditRow.STATUS_SUCCESS;

        try {
            // ── Phase 1: Feature decisions from BQ view ─────────────────────────
            log.info("Phase 1: Reading feature decisions from BigQuery view");
            Dataset<FeatureDecisionRow> featureDecisionDS = bqViewReader.createViewAndRead(spark, config, args);
            featureDecisionDS.cache();
            featureRowCount = featureDecisionDS.count();
            log.info("Feature decisions loaded: {} rows", featureRowCount);

            if (featureRowCount == 0) {
                log.warn("No feature decisions found for process_id={}. Job complete (no-op).", args.getProcessId());
                writeAudit(spark, config, args, jobStart, 0, 0, jobStatus, null);
                return;
            }

            Set<String> lexiconNames = new HashSet<>(
                featureDecisionDS.select("lexiconName").distinct().as(Encoders.STRING()).collectAsList());
            log.info("Distinct lexicon features to load: {} ({})", lexiconNames.size(), lexiconNames);

            Set<String> relevantMessageIds = new HashSet<>(
                featureDecisionDS.select("messageId").distinct().as(Encoders.STRING()).collectAsList());
            log.info("Distinct message IDs to process: {}", relevantMessageIds.size());

            // ── Phase 2: Load .hdb + manifests, broadcast ───────────────────────
            log.info("Phase 2: Loading Hyperscan .hdb files and manifests from GCS");
            Map<String, byte[]> hdbBytes = hdbLoader.loadHdbBytes(lexiconNames, config.getHdbGcsBucket(), config.getHdbGcsPrefix());
            Map<String, Map<Integer, TermManifestEntry>> manifests =
                    hdbLoader.loadManifests(lexiconNames, config.getHdbGcsBucket(), config.getHdbGcsPrefix());

            double totalHdbMb = hdbLoader.totalSizeMb(hdbBytes);
            log.info("Total HDB size to broadcast: {} MB for {} features",
                     String.format("%.1f", totalHdbMb), hdbBytes.size());
            if (totalHdbMb > 2048) {
                log.warn("HDB combined size ({} MB) is very large — consider streaming from GCS " +
                         "on executors instead of broadcasting if OOM errors occur.",
                         String.format("%.1f", totalHdbMb));
            }

            Broadcast<Map<String, byte[]>> broadcastHdb = jsc.broadcast(hdbBytes);
            Broadcast<Map<String, Map<Integer, TermManifestEntry>>> broadcastManifests = jsc.broadcast(manifests);
            log.info("Broadcast complete: {} databases, {} manifests", hdbBytes.size(), manifests.size());

            // ── Phase 3: Read messages ───────────────────────────────────────────
            log.info("Phase 3: Reading messages");
            Broadcast<Set<String>> broadcastMsgIds = jsc.broadcast(relevantMessageIds);
            String messageSourcePath = buildMessageSourcePath(config, args);
            Dataset<Row> messageDS = messageReader.readAndFilter(spark, messageSourcePath, broadcastMsgIds).toDF();

            // ── Phase 4: Group feature decisions by message_id ──────────────────
            log.info("Phase 4: Grouping features by message_id");
            Dataset<Row> groupedFeatures = featureDecisionDS.toDF()
                    .groupBy(col("messageId"))
                    .agg(collect_list(struct(
                            col("messageId").alias("message_id"),
                            col("runDate").alias("run_date"),
                            col("processId").alias("process_id"),
                            col("pipelineExecId").alias("pipeline_exec_id"),
                            col("sentDate").alias("sent_date"),
                            col("messageType").alias("message_type"),
                            col("featureId").alias("feature_id"),
                            col("featureType").alias("feature_type"),
                            col("featureName").alias("feature_name"),
                            col("featureOperator").alias("feature_operator"),
                            col("isNoiseReductionRaw").alias("is_noise_reduction_raw"),
                            col("lexiconName").alias("lexicon_name"),
                            col("fromComposite").alias("from_composite"),
                            col("fmFeatureDefinition").alias("fm_feature_definition")
                    )).alias("features"))
                    .withColumnRenamed("messageId", "msg_id_feat");

            // ── Phase 5: Join messages with grouped features ─────────────────────
            log.info("Phase 5: Joining messages with grouped features");
            Dataset<Row> messageWithFeatures = messageDS
                    .join(groupedFeatures, messageDS.col("message_id").equalTo(groupedFeatures.col("msg_id_feat")), "inner")
                    .drop("msg_id_feat");

            int numPartitions = Math.max(spark.sparkContext().defaultParallelism() * 2, 4);
            messageWithFeatures = messageWithFeatures.repartition(numPartitions, col("message_id"));
            log.info("Processing {} partitions", numPartitions);

            // ── Phase 6: mapPartitions — the single scan pass ────────────────────
            log.info("Phase 6: Running Hyperscan scan (mapPartitions)");
            Dataset<MessageScanResult> scanResults = messageWithFeatures.mapPartitions(
                    new LexiconScanPartitionFunction(broadcastHdb, broadcastManifests, args.getPipelineExecId()),
                    Encoders.bean(MessageScanResult.class)
            );
            scanResults.cache();
            long messageCount = scanResults.count();

            // ── Phase 7: Extract and write the 3 output tables ──────────────────
            log.info("Phase 7: Extracting and writing output tables");
            long hitCount = bqWriter.writeAllOutputs(spark, scanResults, config);

            scanResults.unpersist();
            featureDecisionDS.unpersist();
            broadcastHdb.unpersist();
            broadcastManifests.unpersist();
            broadcastMsgIds.unpersist();

            log.info("Lexicon Scan Engine complete. {} messages processed, {} had hits. Duration: {}ms",
                     messageCount, hitCount, System.currentTimeMillis() - jobStart.getTime());

            writeAudit(spark, config, args, jobStart, featureRowCount, messageCount, jobStatus, null);

        } catch (Exception e) {
            log.error("Lexicon Scan Engine failed: {}", e.getMessage(), e);
            jobStatus = PipelineStageAuditRow.STATUS_FAILED;
            errorMessage = e.getMessage();
            writeAudit(spark, config, args, jobStart, featureRowCount, 0, jobStatus, errorMessage);
            throw new RuntimeException("Lexicon Scan Engine failed", e);
        }
    }

    /**
     * Builds the message source path/URI passed to {@link MessageReader}.
     * For the production {@code AvroMessageReader}, this is a {@code gs://}
     * path with RUN_DATE/PIPELINE_EXEC_ID partitioning; test profiles wire in
     * a {@code JsonMessageReader} together with a local/classpath path via
     * {@code application-test.yml} instead (see {@code AppConfig}).
     */
    private String buildMessageSourcePath(JobConfig config, ScanEngineArgs args) {
        String base = "gs://" + config.getMsgGcsBucket() + "/" + config.getMsgGcsPrefix();
        if (!base.contains("RUN_DATE=")) {
            base = base + "/RUN_DATE=" + args.getRunDate() + "/PIPELINE_EXEC_ID=" + args.getPipelineExecId();
        }
        return base.endsWith("/") ? base + "*.avro" : base + "/*.avro";
    }

    private void writeAudit(SparkSession spark, JobConfig config, ScanEngineArgs args, Timestamp jobStart,
                             long inputCount, long recordCount, String jobStatus, String errorMessage) {
        try {
            bqWriter.writeStageAudit(spark, config, args, jobStart, inputCount, recordCount, jobStatus, errorMessage);
        } catch (Exception auditEx) {
            // Audit failures must never mask the original job outcome — log and continue.
            log.error("Failed to write pipeline_stage_audit: {}", auditEx.getMessage(), auditEx);
        }
    }
}
