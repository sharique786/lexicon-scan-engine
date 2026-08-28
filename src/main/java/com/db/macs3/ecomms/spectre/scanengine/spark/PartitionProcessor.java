package com.db.macs3.ecomms.spectre.scanengine.spark;

import com.db.macs3.ecomms.spectre.scanengine.avro.MessageRowConverter;
import com.db.macs3.ecomms.spectre.scanengine.bq.ViewRowConverter;
import com.db.macs3.ecomms.spectre.scanengine.decision.DecisionTreeEvaluator;
import com.db.macs3.ecomms.spectre.scanengine.decision.FeatureGroupingService;
import com.db.macs3.ecomms.spectre.scanengine.decision.FeatureScanOrchestrator;
import com.db.macs3.ecomms.spectre.scanengine.gcs.GcsClient;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanDatabaseLoader;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.TermMetadataLoader;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitSummaryRow;
import com.db.macs3.ecomms.spectre.scanengine.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.scanengine.model.view.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.scanengine.output.OutputRowBuilder;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The {@code mapPartitions} function that turns one partition's joined
 * (message + its view rows) input into {@link MessageProcessingResult}s.
 *
 * <h2>Why {@code mapPartitions}, not {@code map}</h2>
 * <p>Loading a Hyperscan database (and its accompanying term metadata — see
 * {@link TermMetadataLoader}) is expensive (native library calls, a GCS
 * round-trip on a cache miss — see {@code HyperscanDatabaseLoader}).
 * {@code mapPartitions} runs its function body once PER PARTITION, with the
 * function itself controlling how many input rows it consumes from the
 * supplied iterator — this is what lets exactly ONE
 * {@link HyperscanDatabaseLoader} and ONE {@link TermMetadataLoader} (each
 * with its own cache) be constructed per partition and reused across every
 * message in it, rather than being rebuilt (and their caches thrown away)
 * for every single message the way a plain {@code map} closure would if it
 * tried to do the same lazy-init pattern per element.
 *
 * <h2>Driver load</h2>
 * <p>This class's {@link #call} runs entirely on an executor, once per
 * partition. It never calls back to the driver and never triggers a Spark
 * action — {@link GcsClient}, {@link HyperscanDatabaseLoader},
 * {@link TermMetadataLoader}, and every scanned message stay local to the
 * executor JVM processing that partition. Only the two small
 * {@code feature -> path} maps (one for {@code .hdb} files, one for
 * term-metadata JSON files — see {@code HyperscanPathResolver}) are supplied
 * from the driver, via Spark broadcast variables (see
 * {@code ScanEngineJobRunner}), not recomputed per partition.
 *
 * <p>Not independently executable-verified in this project's development
 * sandbox — see {@code GcsClient} class Javadoc.
 */
public final class PartitionProcessor implements MapPartitionsFunction<Row, MessageProcessingResult> {

    private static final Logger log = LoggerFactory.getLogger(PartitionProcessor.class);

    private final org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToPathBroadcast;
    private final org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToMetadataPathBroadcast;
    private final Long maxAttachmentSizeBytes;
    private final int maxCachedDatabasesPerPartition;

    /**
     * @param featureToPathBroadcast          a Spark {@link org.apache.spark.broadcast.Broadcast} of the
     *                                          feature → {@code .hdb} path map — genuinely broadcast (sent
     *                                          once per executor JVM, not re-serialized per task) — see
     *                                          {@code ScanEngineJobRunner} for construction
     * @param featureToMetadataPathBroadcast   the same, for the feature → term-metadata JSON path map —
     *                                          see {@link TermMetadataLoader} class Javadoc for why this
     *                                          second broadcast now exists
     */
    public PartitionProcessor(org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToPathBroadcast,
                               org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToMetadataPathBroadcast,
                               Long maxAttachmentSizeBytes, int maxCachedDatabasesPerPartition) {
        this.featureToPathBroadcast = featureToPathBroadcast;
        this.featureToMetadataPathBroadcast = featureToMetadataPathBroadcast;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
        this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition;
    }

    @Override
    public Iterator<MessageProcessingResult> call(Iterator<Row> partitionRows) {
        // Constructed ONCE per partition — see class Javadoc. Both broadcast .value() calls read
        // the executor-local copy Spark already delivered via its broadcast mechanism — this does
        // not re-fetch or re-serialize anything per partition/task.
        GcsClient gcsClient = new GcsClient();
        try (HyperscanDatabaseLoader databaseLoader = new HyperscanDatabaseLoader(
                featureToPathBroadcast.value(), gcsClient::openStream, maxCachedDatabasesPerPartition);
             TermMetadataLoader metadataLoader = new TermMetadataLoader(
                     featureToMetadataPathBroadcast.value(), gcsClient::openStream, maxCachedDatabasesPerPartition)) {

            FeatureScanOrchestrator orchestrator =
                    new FeatureScanOrchestrator(databaseLoader, metadataLoader, maxAttachmentSizeBytes);
            List<MessageProcessingResult> results = new ArrayList<>();

            while (partitionRows.hasNext()) {
                Row row = partitionRows.next();
                results.add(processOneRow(row, orchestrator));
            }
            return results.iterator();
        }
    }

    private MessageProcessingResult processOneRow(Row row, FeatureScanOrchestrator orchestrator) {
        boolean restricted = row.getAs("restricted");
        String datasetPartitionValue = row.getAs("dataset_partition_value_for_output");
        ScanMessage message = MessageRowConverter.fromRow(row, row.getAs("dataset_id"), restricted);

        try {
            List<Row> featureRows = row.getList(row.fieldIndex("features"));
            List<FeatureDecisionRow> viewRows = new ArrayList<>(featureRows.size());
            for (Row featureRow : featureRows) {
                viewRows.add(ViewRowConverter.fromRow(featureRow));
            }

            List<FeatureGroup> orderedGroups = FeatureGroupingService.groupAndOrder(viewRows);
            DecisionTreeEvaluator.FeatureRowScanner scanner = orchestrator.scannerFor(message);
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate(message.messageId(), orderedGroups, scanner);

            String processId = viewRows.get(0).processId();
            // pipelineExecId/createdBy are not view columns — carried through as extra columns
            // attached during the join stage (see LexiconScanEngineJob), not read from the view itself.
            String pipelineExecId = row.getAs("pipeline_exec_id_for_output");
            String featureTaggingType = viewRows.get(0).featureTaggingType();
            String createdBy = row.getAs("created_by_for_output");
            Instant now = Instant.now();

            LexiconHitSummaryRow summaryRow = OutputRowBuilder.buildSummaryRow(
                    message.messageId(), processId, pipelineExecId, evaluation, createdBy, now);
            LexiconHitDetailRow detailRow = OutputRowBuilder.buildDetailRow(
                    message.messageId(), processId, pipelineExecId, datasetPartitionValue, evaluation, createdBy, now);
            FeatureHitSummaryRow featureHitSummaryRow = OutputRowBuilder.buildFeatureHitSummaryRow(
                    message.messageId(), datasetPartitionValue, pipelineExecId, processId, featureTaggingType,
                    evaluation, createdBy, now);

            return MessageProcessingResult.success(
                    message.messageId(), restricted, datasetPartitionValue, summaryRow, detailRow, featureHitSummaryRow);

        } catch (Exception e) {
            // Requirement 3, "Other errors": a single message's processing failure must NOT
            // fail the whole job — recorded here for pipeline_record_audit instead (see
            // LexiconScanEngineJob, which writes every isError() result there).
            log.warn("Processing failed for message_id={}: {}", message.messageId(), e.getMessage(), e);
            return MessageProcessingResult.failure(message.messageId(), restricted, datasetPartitionValue, e.toString());
        }
    }
}
