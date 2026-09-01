package com.db.macs3.ecomms.spectre.scanengine.spark;

import com.db.macs3.ecomms.spectre.scanengine.avro.MessageRowConverter;
import com.db.macs3.ecomms.spectre.scanengine.bq.ViewRowConverter;
import com.db.macs3.ecomms.spectre.scanengine.decision.DecisionTreeEvaluator;
import com.db.macs3.ecomms.spectre.scanengine.decision.FeatureGroupingService;
import com.db.macs3.ecomms.spectre.scanengine.decision.FeatureScanOrchestrator;
import com.db.macs3.ecomms.spectre.scanengine.gcs.GcsClient;
import com.db.macs3.ecomms.spectre.scanengine.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.scanengine.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.scanengine.model.feature.FeatureDefinition;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code mapPartitions} function that turns one partition's joined
 * (message + its view rows) input into {@link MessageProcessingResult}s.
 *
 * <h2>Why {@code mapPartitions}, not {@code map}</h2>
 * <p>Loading a Hyperscan database and its accompanying term metadata — both
 * now extracted from the same GCS zip bundle, see {@link HyperscanBundleLoader}
 * class Javadoc — is expensive (native library calls, a GCS round-trip and a
 * zip extraction on a cache miss). {@code mapPartitions} runs its function
 * body once PER PARTITION, with the function itself controlling how many
 * input rows it consumes from the supplied iterator — this is what lets
 * exactly ONE {@link HyperscanBundleLoader} (with its own cache) be
 * constructed per partition and reused across every message in it, rather
 * than being rebuilt (and its cache thrown away) for every single message
 * the way a plain {@code map} closure would if it tried to do the same
 * lazy-init pattern per element.
 *
 * <h2>Driver load</h2>
 * <p>This class's {@link #call} runs entirely on an executor, once per
 * partition. It never calls back to the driver and never triggers a Spark
 * action — {@link GcsClient}, {@link HyperscanBundleLoader}, and every
 * scanned message stay local to the executor JVM processing that partition.
 * Only the one small {@code feature -> zip path} map (see
 * {@code HyperscanPathResolver#buildZipPath}) is supplied from the driver,
 * via a Spark broadcast variable (see {@code ScanEngineJobRunner}), not
 * recomputed per partition.
 *
 * <h2>Bounded lookahead prefetch — not a full-partition materialisation</h2>
 * <p>{@link #call} peeks at most {@link #PREFETCH_LOOKAHEAD_ROWS} rows ahead
 * (a small, FIXED buffer) purely to discover which distinct features the
 * START of this partition will need, so {@link HyperscanBundleLoader#prefetch}
 * can warm their bundles CONCURRENTLY (see that method's Javadoc) instead of
 * paying for each one's cold-cache GCS round-trip serially, one at a time,
 * the way the first several messages in a partition otherwise would.
 * Deliberately NOT a full-partition materialisation into a {@code List<Row>}:
 * a partition can hold many thousands of messages, some individually large
 * (big attachments) — buffering the WHOLE partition to compute a fully
 * exhaustive distinct-feature set would work directly against the bounded,
 * one-row-at-a-time memory footprint {@code mapPartitions}'s iterator-based
 * contract is meant to provide, in exactly the "some messages can be very
 * large" scenario this needs to stay safe under. The rest of the partition,
 * beyond the lookahead window, is still processed by streaming the iterator
 * exactly as before.
 */
public final class PartitionProcessor implements MapPartitionsFunction<Row, MessageProcessingResult> {

    private static final Logger log = LoggerFactory.getLogger(PartitionProcessor.class);

    /** See class Javadoc "Bounded lookahead prefetch" — deliberately small and fixed, not partition-sized. */
    private static final int PREFETCH_LOOKAHEAD_ROWS = 200;

    private final org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToZipPathBroadcast;
    private final Long maxAttachmentSizeBytes;
    private final int maxCachedDatabasesPerPartition;

    /**
     * @param featureToZipPathBroadcast   a Spark {@link org.apache.spark.broadcast.Broadcast} of the
     *                                     feature → {@code .zip} bundle path map — genuinely broadcast
     *                                     (sent once per executor JVM, not re-serialized per task) — see
     *                                     {@code ScanEngineJobRunner} for construction
     */
    public PartitionProcessor(org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToZipPathBroadcast,
                               Long maxAttachmentSizeBytes, int maxCachedDatabasesPerPartition) {
        this.featureToZipPathBroadcast = featureToZipPathBroadcast;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
        this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition;
    }

    @Override
    public Iterator<MessageProcessingResult> call(Iterator<Row> partitionRows) {
        // Constructed ONCE per partition — see class Javadoc. The broadcast .value() call reads
        // the executor-local copy Spark already delivered via its broadcast mechanism — this does
        // not re-fetch or re-serialize anything per partition/task.
        GcsClient gcsClient = new GcsClient();
        try (HyperscanBundleLoader bundleLoader = new HyperscanBundleLoader(
                     featureToZipPathBroadcast.value(), gcsClient::openStream, maxCachedDatabasesPerPartition);
             FeatureScanOrchestrator orchestrator = new FeatureScanOrchestrator(bundleLoader, maxAttachmentSizeBytes)) {

            // Bounded lookahead + concurrent prefetch — see class Javadoc. lookaheadBuffer holds
            // at most PREFETCH_LOOKAHEAD_ROWS raw rows, never the whole partition.
            List<Row> lookaheadBuffer = new ArrayList<>(PREFETCH_LOOKAHEAD_ROWS);
            Set<String> distinctFeatures = new LinkedHashSet<>();
            while (partitionRows.hasNext() && lookaheadBuffer.size() < PREFETCH_LOOKAHEAD_ROWS) {
                Row row = partitionRows.next();
                lookaheadBuffer.add(row);
                collectDistinctFeatures(row, distinctFeatures);
            }
            bundleLoader.prefetch(distinctFeatures);

            List<MessageProcessingResult> results = new ArrayList<>();
            for (Row row : lookaheadBuffer) {
                results.add(processOneRow(row, orchestrator));
            }
            while (partitionRows.hasNext()) {
                results.add(processOneRow(partitionRows.next(), orchestrator));
            }
            return results.iterator();
        }
    }

    /**
     * Best-effort only (see {@code HyperscanBundleLoader.prefetch} class
     * Javadoc): a malformed feature definition here is silently skipped —
     * it will surface properly, with full per-message error isolation, when
     * this row is actually processed by {@link #processOneRow} below.
     */
    private static void collectDistinctFeatures(Row row, Set<String> distinctFeaturesOut) {
        List<Row> featureRows = row.getList(row.fieldIndex(JoinedRowColumns.FEATURES));
        for (Row featureRow : featureRows) {
            try {
                String featureDefinitionJson = ViewRowConverter.fromRow(featureRow).featureDefinitionJson();
                distinctFeaturesOut.add(FeatureDefinition.parse(featureDefinitionJson).body().feature());
            } catch (RuntimeException e) {
                log.debug("Could not parse a feature definition during prefetch lookahead — "
                        + "will be handled normally during real processing: {}", e.getMessage());
            }
        }
    }

    private MessageProcessingResult processOneRow(Row row, FeatureScanOrchestrator orchestrator) {
        boolean restricted = row.getAs(JoinedRowColumns.RESTRICTED);
        String datasetPartitionValue = row.getAs(JoinedRowColumns.DATASET_PARTITION_VALUE_FOR_OUTPUT);
        ScanMessage message = MessageRowConverter.fromRow(row, row.getAs(JoinedRowColumns.DATASET_ID), restricted);

        try {
            List<Row> featureRows = row.getList(row.fieldIndex(JoinedRowColumns.FEATURES));
            List<FeatureDecisionRow> viewRows = new ArrayList<>(featureRows.size());
            for (Row featureRow : featureRows) {
                viewRows.add(ViewRowConverter.fromRow(featureRow));
            }

            List<FeatureGroup> orderedGroups = FeatureGroupingService.groupAndOrder(viewRows);
            DecisionTreeEvaluator.FeatureRowScanner scanner = orchestrator.scannerFor(message);
            MessageEvaluationResult evaluation = DecisionTreeEvaluator.evaluate(message.messageId(), orderedGroups, scanner);

            String processId = viewRows.getFirst().processId();
            // pipelineExecId/createdBy are not view columns — carried through as extra columns
            // attached during the join stage (see ScanEngineJobRunner), not read from the view itself.
            String pipelineExecId = row.getAs(JoinedRowColumns.PIPELINE_EXEC_ID_FOR_OUTPUT);
            String featureTaggingType = viewRows.getFirst().featureTaggingType();
            String createdBy = row.getAs(JoinedRowColumns.CREATED_BY_FOR_OUTPUT);
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
            // A single message's processing failure must NOT fail the whole job — recorded
            // here for pipeline_record_audit instead (see ScanEngineJobRunner.writeOutputs,
            // which writes every isError() result there).
            log.warn("Processing failed for message_id={}: {}", message.messageId(), e.getMessage(), e);
            return MessageProcessingResult.failure(message.messageId(), restricted, datasetPartitionValue, e.toString());
        }
    }
}
