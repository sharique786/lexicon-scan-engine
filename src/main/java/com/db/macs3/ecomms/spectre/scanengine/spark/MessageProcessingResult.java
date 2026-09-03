package com.db.macs3.ecomms.spectre.scanengine.spark;

import com.db.macs3.ecomms.spectre.scanengine.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.scanengine.model.output.LexiconHitSummaryRow;

import java.io.Serializable;
import java.util.Objects;

/**
 * The per-message result of {@link PartitionProcessor} — either a
 * successfully-built set of output rows, or an error to be recorded in
 * {@code pipeline_record_audit}, so that a single message's processing
 * failure never fails the whole job.
 *
 * <p>Exactly one of ({@link #summaryRow}, {@link #featureHitSummaryRow}) vs
 * {@link #errorMessage} is meaningful for a given instance — see
 * {@link #isError()}. {@link #detailRow} may be null even on success (a
 * message with nothing surviving disclaimer suppression, or one
 * short-circuited by noise reduction — see {@code OutputRowBuilder#buildDetailRow}).
 *
 * <p>Serializable: this is the element type Spark's {@code mapPartitions}
 * output {@code Dataset} carries via {@code Encoders.kryo}.
 */
public final class MessageProcessingResult implements Serializable {

    private final String messageId;
    private final boolean restricted;
    private final String datasetPartitionValue;
    private final LexiconHitSummaryRow summaryRow;
    private final LexiconHitDetailRow detailRow;
    private final FeatureHitSummaryRow featureHitSummaryRow;
    private final String errorMessage;

    /**
     * @param messageId               the message this result is for
     * @param restricted                which output table ({@code lexicon-hit-restricted} vs
     *                                 {@code -unrestricted}) {@link #detailRow} belongs to
     * @param datasetPartitionValue     carried through for the audit/error path
     * @param summaryRow                 null iff {@link #isError()}
     * @param detailRow                   may be null on success (see class Javadoc); always null on error
     * @param featureHitSummaryRow       null iff {@link #isError()}
     * @param errorMessage                null on success; the failure detail otherwise
     */
    public MessageProcessingResult(String messageId, boolean restricted, String datasetPartitionValue,
                                    LexiconHitSummaryRow summaryRow, LexiconHitDetailRow detailRow,
                                    FeatureHitSummaryRow featureHitSummaryRow, String errorMessage) {
        this.messageId = messageId;
        this.restricted = restricted;
        this.datasetPartitionValue = datasetPartitionValue;
        this.summaryRow = summaryRow;
        this.detailRow = detailRow;
        this.featureHitSummaryRow = featureHitSummaryRow;
        this.errorMessage = errorMessage;
    }

    public String messageId() { return messageId; }
    public boolean restricted() { return restricted; }
    public String datasetPartitionValue() { return datasetPartitionValue; }
    public LexiconHitSummaryRow summaryRow() { return summaryRow; }
    public LexiconHitDetailRow detailRow() { return detailRow; }
    public FeatureHitSummaryRow featureHitSummaryRow() { return featureHitSummaryRow; }
    public String errorMessage() { return errorMessage; }

    public boolean isError() {
        return errorMessage != null;
    }

    public static MessageProcessingResult success(String messageId, boolean restricted, String datasetPartitionValue,
                                                    LexiconHitSummaryRow summaryRow, LexiconHitDetailRow detailRow,
                                                    FeatureHitSummaryRow featureHitSummaryRow) {
        return new MessageProcessingResult(
                messageId, restricted, datasetPartitionValue, summaryRow, detailRow, featureHitSummaryRow, null);
    }

    public static MessageProcessingResult failure(String messageId, boolean restricted, String datasetPartitionValue,
                                                    String errorMessage) {
        return new MessageProcessingResult(
                messageId, restricted, datasetPartitionValue, null, null, null, errorMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageProcessingResult)) {
            return false;
        }
        MessageProcessingResult other = (MessageProcessingResult) o;
        return restricted == other.restricted
                && Objects.equals(messageId, other.messageId)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(summaryRow, other.summaryRow)
                && Objects.equals(detailRow, other.detailRow)
                && Objects.equals(featureHitSummaryRow, other.featureHitSummaryRow)
                && Objects.equals(errorMessage, other.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, restricted, datasetPartitionValue, summaryRow, detailRow,
                featureHitSummaryRow, errorMessage);
    }

    @Override
    public String toString() {
        return "MessageProcessingResult[messageId=" + messageId + ", restricted=" + restricted
                + ", datasetPartitionValue=" + datasetPartitionValue + ", summaryRow=" + summaryRow
                + ", detailRow=" + detailRow + ", featureHitSummaryRow=" + featureHitSummaryRow
                + ", errorMessage=" + errorMessage + "]";
    }
}
