package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.avro.MessageRowConverter;
import com.db.macs3.ecomms.spectre.bq.ViewRowConverter;
import com.db.macs3.ecomms.spectre.decision.DecisionTreeEvaluator;
import com.db.macs3.ecomms.spectre.decision.FeatureGroupingService;
import com.db.macs3.ecomms.spectre.decision.FeatureScanOrchestrator;
import com.db.macs3.ecomms.spectre.gcs.GcsClient;
import com.db.macs3.ecomms.spectre.hyperscan.HyperscanBundleLoader;
import com.db.macs3.ecomms.spectre.hyperscan.SharedBundleCache;
import com.db.macs3.ecomms.spectre.model.decision.FeatureGroup;
import com.db.macs3.ecomms.spectre.model.decision.MessageEvaluationResult;
import com.db.macs3.ecomms.spectre.model.feature.FeatureDefinition;
import com.db.macs3.ecomms.spectre.model.message.MessageAttachment;
import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.view.FeatureDecisionRow;
import com.db.macs3.ecomms.spectre.output.OutputRowBuilder;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
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

    /**
     * See class Javadoc "Bounded lookahead prefetch" — deliberately small and fixed, not partition-sized.
     */
    // 64, not more: every buffered row holds its whole message (attachments included) on the heap at once, and
    // after the feature-set sort the first rows of a partition mostly share one feature set anyway.
    private static final int PREFETCH_LOOKAHEAD_ROWS = 64;

    private final org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToZipPathBroadcast;
    private final Long maxAttachmentSizeBytes;
    private final int maxCachedDatabasesPerPartition;
    /** Idle-bundle cap of the executor-wide {@code SharedBundleCache}; negative = no sharing (private loads). */
    private final int maxSharedIdleBundles;
    /** Unique per job run — namespaces shared-cache keys so two runs in one JVM never mix bundles. */
    private final String runId;

    /**
     * @param featureToZipPathBroadcast a Spark {@link org.apache.spark.broadcast.Broadcast} of the
     *                                  feature → {@code .zip} bundle path map — genuinely broadcast
     *                                  (sent once per executor JVM, not re-serialized per task) — see
     *                                  {@code ScanEngineJobRunner} for construction
     */
    public PartitionProcessor(org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToZipPathBroadcast,
                              Long maxAttachmentSizeBytes, int maxCachedDatabasesPerPartition) {
        this(featureToZipPathBroadcast, maxAttachmentSizeBytes, maxCachedDatabasesPerPartition, -1, "");
    }

    /**
     * @param maxSharedIdleBundles &gt;= 0 shares bundles between the executor's tasks through
     *                             {@link SharedBundleCache#forExecutor}; negative loads privately per partition
     * @param runId                unique per job run (see field)
     */
    public PartitionProcessor(org.apache.spark.broadcast.Broadcast<Map<String, String>> featureToZipPathBroadcast,
                              Long maxAttachmentSizeBytes, int maxCachedDatabasesPerPartition,
                              int maxSharedIdleBundles, String runId) {
        this.featureToZipPathBroadcast = featureToZipPathBroadcast;
        this.maxAttachmentSizeBytes = maxAttachmentSizeBytes;
        this.maxCachedDatabasesPerPartition = maxCachedDatabasesPerPartition;
        this.maxSharedIdleBundles = maxSharedIdleBundles;
        this.runId = runId;
    }

    @Override
    public Iterator<MessageProcessingResult> call(Iterator<Row> partitionRows) {
        int partitionId = TaskContext.getPartitionId();
        log.info("Partition {}: processing starting", partitionId);
        Instant partitionStart = Instant.now();
        // Constructed ONCE per partition — see class Javadoc. The broadcast .value() call reads
        // the executor-local copy Spark already delivered via its broadcast mechanism — this does
        // not re-fetch or re-serialize anything per partition/task.
        GcsClient gcsClient = new GcsClient();
        HyperscanBundleLoader bundleLoader = null;
        FeatureScanOrchestrator orchestrator = null;
        try {
            SharedBundleCache sharedCache = maxSharedIdleBundles >= 0
                    ? SharedBundleCache.forExecutor(maxSharedIdleBundles) : null;
            bundleLoader = new HyperscanBundleLoader(featureToZipPathBroadcast.value(), gcsClient::openStream,
                    maxCachedDatabasesPerPartition, sharedCache, runId);
            orchestrator = new FeatureScanOrchestrator(bundleLoader, maxAttachmentSizeBytes);

            // Safety net: StreamingResults only closes on exhaustion or an exception thrown from next().
            // If Spark stops consuming early (task killed/interrupted, downstream failure, speculative
            // duplicate) the native Scanner scratch and every cached Hyperscan database would leak for
            // the life of the executor. Task completion always fires; close() is idempotent.
            TaskContext taskContext = TaskContext.get();
            if (taskContext != null) {
                final FeatureScanOrchestrator orchestratorToClose = orchestrator;
                final HyperscanBundleLoader loaderToClose = bundleLoader;
                taskContext.addTaskCompletionListener(ctx -> {
                    closeQuietly(orchestratorToClose);
                    closeQuietly(loaderToClose);
                });
            }

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

            // LAZY: one result is built per next() call, so only one message's matches/JSON is on the
            // heap at a time (Spark consumes/serialises each result before pulling the next). The
            // orchestrator/loader are closed when the iterator is exhausted or fails.
            return new StreamingResults(partitionId, partitionStart, lookaheadBuffer.iterator(), partitionRows,
                    orchestrator, bundleLoader);
        } catch (RuntimeException e) {
            closeQuietly(orchestrator);
            closeQuietly(bundleLoader);
            // Per-message failures are already isolated inside processOneRow (see below) — reaching
            // here means something broke at the PARTITION level instead (bundle loader construction,
            // prefetch, or an unanticipated error outside the per-row try/catch), which is fatal to
            // this whole partition's task and must fail the Spark job rather than be silently
            // swallowed. Logged with elapsed time before rethrow so this partition is identifiable
            // in the driver's aggregated task-failure output.
            log.error("Partition {}: processing failed after {}ms: {}",
                    partitionId, Duration.between(partitionStart, Instant.now()).toMillis(), e.getMessage(), e);
            throw e;
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close {}: {}", closeable.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    /**
     * Lazily processes the partition one row per {@link #next()}, then closes the partition-scoped
     * native resources (Scanner, bundle cache) once the input is exhausted or processing throws.
     */
    private final class StreamingResults implements Iterator<MessageProcessingResult> {
        private final int partitionId;
        private final Instant partitionStart;
        private final Iterator<Row> lookahead;
        private final Iterator<Row> rest;
        private final FeatureScanOrchestrator orchestrator;
        private final HyperscanBundleLoader bundleLoader;
        private long processed;
        private long failed;
        private boolean closed;

        StreamingResults(int partitionId, Instant partitionStart, Iterator<Row> lookahead, Iterator<Row> rest,
                         FeatureScanOrchestrator orchestrator, HyperscanBundleLoader bundleLoader) {
            this.partitionId = partitionId;
            this.partitionStart = partitionStart;
            this.lookahead = lookahead;
            this.rest = rest;
            this.orchestrator = orchestrator;
            this.bundleLoader = bundleLoader;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            boolean more = lookahead.hasNext() || rest.hasNext();
            if (!more) {
                close();
                log.info("Partition {}: processing completed in {}ms — {} message(s) processed, {} failed",
                        partitionId, Duration.between(partitionStart, Instant.now()).toMillis(), processed, failed);
            }
            return more;
        }

        @Override
        public MessageProcessingResult next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            try {
                Row row = lookahead.hasNext() ? lookahead.next() : rest.next();
                MessageProcessingResult result = processOneRow(row, orchestrator);
                processed++;
                if (result.isError()) {
                    failed++;
                }
                return result;
            } catch (RuntimeException | Error e) {
                close();
                log.error("Partition {}: processing failed after {}ms: {}",
                        partitionId, Duration.between(partitionStart, Instant.now()).toMillis(), e.getMessage(), e);
                throw e;
            }
        }

        private void close() {
            if (!closed) {
                closed = true;
                closeQuietly(orchestrator);
                closeQuietly(bundleLoader);
            }
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
                String featureDefinitionJson = ViewRowConverter.fromRow(featureRow).getFeatureDefinitionJson();
                distinctFeaturesOut.add(FeatureDefinition.parseCached(featureDefinitionJson).getBody().getLexiconName());
            } catch (RuntimeException e) {
                log.debug("Could not parse a feature definition during prefetch lookahead — "
                        + "will be handled normally during real processing: {}", e.getMessage());
            }
        }
    }

    private MessageProcessingResult processOneRow(Row row, FeatureScanOrchestrator orchestrator) {
        boolean restricted = row.getAs(JoinedRowColumns.RESTRICTED);
        String datasetPartitionValue = row.getAs(JoinedRowColumns.DATASET_PARTITION_VALUE_FOR_OUTPUT);
        ScanMessage message = MessageRowConverter.fromRow(row, datasetPartitionValue, restricted);

        MessageProcessingResult result;
        try {
            result = buildSuccessResult(row, orchestrator, message, restricted, datasetPartitionValue);
        } catch (Exception e) {
            // A single message's processing failure must NOT fail the whole job — recorded
            // here for pipeline_record_audit instead (see ScanEngineJobRunner.writeOutputs,
            // which writes every result there, success and failure alike).
            log.warn("Processing failed for message_id={}: {}", message.getMessageId(), e.getMessage(), e);
            result = MessageProcessingResult.failure(message.getMessageId(), restricted, datasetPartitionValue, e.toString());
        }
        // source_name/sent_date/run_date come from the AVRO message, and pipeline_record_audit wants
        // them for a failed record just as much as for a successful one.
        return result.withMessageAttributes(message);
    }

    private MessageProcessingResult buildSuccessResult(Row row, FeatureScanOrchestrator orchestrator,
                                                        ScanMessage message, boolean restricted,
                                                        String datasetPartitionValue) {
        // dataset_partition_value is REQUIRED (NOT NULL, DATE) on all 4 output tables — a null/
        // malformed value here must fail THIS message only (caught by the caller), not the whole
        // partition task the way an uncaught LocalDate.parse failure outside that try block would.
        // Same for message_id: the BigQuery schema requires it, and a null here would otherwise slip
        // all the way to the BQ write stage before failing, taking the ENTIRE batch of rows down with
        // a cryptic Catalyst EXPRESSION_ENCODING_FAILED error instead of being isolated to one message.
        requireNonBlank(message.getMessageId(), "AVRO record has a null/blank message_id — cannot process");
        requireNonBlank(datasetPartitionValue, "dataset_partition_value_for_output is null/blank for message_id="
                + message.getMessageId());
        LocalDate datasetPartitionValueDate = parseDatasetPartitionValueDate(datasetPartitionValue, message.getMessageId());

        List<FeatureDecisionRow> viewRows = readViewRows(row);

        String processId = viewRows.getFirst().getProcessId();
        // pipelineExecId/createdBy are not view columns — carried through as extra columns
        // attached during the join stage (see ScanEngineJobRunner), not read from the view itself.
        String pipelineExecId = row.getAs(JoinedRowColumns.PIPELINE_EXEC_ID_FOR_OUTPUT);
        String createdBy = row.getAs(JoinedRowColumns.CREATED_BY_FOR_OUTPUT);
        // process_id/pipeline_exec_id/created_by are REQUIRED on every output table — same
        // fail-this-message-only reasoning as the message_id/dataset_partition_value checks above.
        requireNonBlank(processId, "view row has a null/blank process_id for message_id=" + message.getMessageId());
        requireNonBlank(pipelineExecId,
                "pipeline_exec_id_for_output is null/blank for message_id=" + message.getMessageId());
        requireNonBlank(createdBy, "created_by_for_output is null/blank for message_id=" + message.getMessageId());

        MessageEvaluationResult evaluation = evaluateMessage(message, orchestrator, viewRows);

        return buildOutputResult(message, restricted, datasetPartitionValue, datasetPartitionValueDate,
                processId, pipelineExecId, createdBy, viewRows.getFirst().getFeatureTaggingType(), evaluation);
    }

    private static void requireNonBlank(String value, String errorMessage) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private static LocalDate parseDatasetPartitionValueDate(String datasetPartitionValue, String messageId) {
        try {
            return LocalDate.parse(datasetPartitionValue);
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalStateException("dataset_partition_value_for_output='" + datasetPartitionValue
                    + "' is not a valid ISO date for message_id=" + messageId, e);
        }
    }

    private static List<FeatureDecisionRow> readViewRows(Row row) {
        List<Row> featureRows = row.getList(row.fieldIndex(JoinedRowColumns.FEATURES));
        List<FeatureDecisionRow> viewRows = new ArrayList<>(featureRows.size());
        for (Row featureRow : featureRows) {
            viewRows.add(ViewRowConverter.fromRow(featureRow));
        }
        return viewRows;
    }

    private static MessageEvaluationResult evaluateMessage(ScanMessage message, FeatureScanOrchestrator orchestrator,
                                                            List<FeatureDecisionRow> viewRows) {
        List<FeatureGroup> orderedGroups = FeatureGroupingService.groupAndOrder(viewRows);
        DecisionTreeEvaluator.FeatureRowScanner scanner = orchestrator.scannerFor(message);
        return DecisionTreeEvaluator.evaluate(message.getMessageId(), orderedGroups, scanner);
    }

    private static MessageProcessingResult buildOutputResult(ScanMessage message, boolean restricted,
                                                              String datasetPartitionValue, LocalDate datasetPartitionValueDate,
                                                              String processId, String pipelineExecId, String createdBy,
                                                              String featureTaggingType, MessageEvaluationResult evaluation) {
        Instant now = Instant.now();
        LexiconHitSummaryRow summaryRow = OutputRowBuilder.buildSummaryRow(
                message.getMessageId(), processId, pipelineExecId, datasetPartitionValueDate, evaluation, createdBy, now);
        Map<String, String> attachmentNames = new HashMap<>();
        for (MessageAttachment attachment : message.getAttachments()) {
            if (attachment.getAttachmentId() != null && attachment.getFileName() != null) {
                attachmentNames.put(attachment.getAttachmentId(), attachment.getFileName());
            }
        }
        LexiconHitDetailRow detailRow = OutputRowBuilder.buildDetailRow(
                message.getMessageId(), processId, pipelineExecId, datasetPartitionValueDate, evaluation, createdBy, now,
                attachmentNames);
        FeatureHitSummaryRow featureHitSummaryRow = OutputRowBuilder.buildFeatureHitSummaryRow(
                message.getMessageId(), datasetPartitionValueDate, pipelineExecId, processId, featureTaggingType,
                evaluation, createdBy, now);

        return MessageProcessingResult.success(
                message.getMessageId(), restricted, datasetPartitionValue, summaryRow, detailRow, featureHitSummaryRow);
    }
}
