package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.model.output.FeatureHitSummaryRow;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitDetailRow;
import com.db.macs3.ecomms.spectre.model.message.ScanMessage;
import com.db.macs3.ecomms.spectre.model.output.LexiconHitSummaryRow;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * The per-message result of {@link PartitionProcessor} — either a
 * successfully-built set of output rows, or an error to be recorded in
 * {@code pipeline_record_audit}, so that a single message's processing
 * failure never fails the whole job.
 *
 * <p>Exactly one of ({@link #getSummaryRow()}, {@link #getFeatureHitSummaryRow()}) vs
 * {@link #getErrorMessage()} is meaningful for a given instance — see
 * {@link #isError()}. {@link #getDetailRow()} may be null even on success (a
 * message with nothing surviving disclaimer suppression, or one
 * short-circuited by noise reduction — see {@code OutputRowBuilder#buildDetailRow}).
 *
 * <p>Serializable: this is the element type Spark's {@code mapPartitions}
 * output {@code Dataset} carries via {@code Encoders.kryo}.
 */
public class MessageProcessingResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private boolean restricted;
    private String datasetPartitionValue;
    private LexiconHitSummaryRow summaryRow;
    private LexiconHitDetailRow detailRow;
    private FeatureHitSummaryRow featureHitSummaryRow;
    private String errorMessage;
    private String sourceName;
    private Instant sentDate;
    private String runDate;

    public MessageProcessingResult() {
    }

    /**
     * @param messageId             the message this result is for
     * @param restricted            which output table ({@code lexicon-hit-restricted} vs
     *                              {@code -unrestricted}) {@link #getDetailRow()} belongs to
     * @param datasetPartitionValue carried through for the audit/error path
     * @param summaryRow            null iff {@link #isError()}
     * @param detailRow             may be null on success (see class Javadoc); always null on error
     * @param featureHitSummaryRow  null iff {@link #isError()}
     * @param errorMessage          null on success; the failure detail otherwise
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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public boolean isRestricted() {
        return restricted;
    }

    public void setRestricted(boolean restricted) {
        this.restricted = restricted;
    }

    public String getDatasetPartitionValue() {
        return datasetPartitionValue;
    }

    public void setDatasetPartitionValue(String datasetPartitionValue) {
        this.datasetPartitionValue = datasetPartitionValue;
    }

    public LexiconHitSummaryRow getSummaryRow() {
        return summaryRow;
    }

    public void setSummaryRow(LexiconHitSummaryRow summaryRow) {
        this.summaryRow = summaryRow;
    }

    public LexiconHitDetailRow getDetailRow() {
        return detailRow;
    }

    public void setDetailRow(LexiconHitDetailRow detailRow) {
        this.detailRow = detailRow;
    }

    public FeatureHitSummaryRow getFeatureHitSummaryRow() {
        return featureHitSummaryRow;
    }

    public void setFeatureHitSummaryRow(FeatureHitSummaryRow featureHitSummaryRow) {
        this.featureHitSummaryRow = featureHitSummaryRow;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isError() {
        return errorMessage != null;
    }

    /**
     * @return {@code source.source_name} of the message this result is for — null if absent
     */
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return {@code message.metadata.start_time_utc} of the message this result is for — null if
     * absent or unparseable
     */
    public Instant getSentDate() {
        return sentDate;
    }

    public void setSentDate(Instant sentDate) {
        this.sentDate = sentDate;
    }

    /**
     * @return {@code processing.run_date} of the message this result is for, as ISO-8601
     * {@code yyyy-MM-dd} text (the audit column is STRING; the AVRO field is a {@code date}) — null if absent
     */
    public String getRunDate() {
        return runDate;
    }

    public void setRunDate(String runDate) {
        this.runDate = runDate;
    }

    /**
     * Copies the three {@code pipeline_record_audit} attributes this result carries
     * ({@code source_name}, {@code sent_date}, {@code run_date}) from {@code message}, leaving each
     * null when the message lacks the AVRO block it lives in. Applied to success AND failure results
     * alike — the audit row needs them either way.
     *
     * @return this result, for chaining
     */
    public MessageProcessingResult withMessageAttributes(ScanMessage message) {
        this.sourceName = message.getSource() == null ? null : message.getSource().getSourceName();
        this.sentDate = message.getMetadata() == null ? null : message.getMetadata().getStartTimeUtc();
        LocalDate messageRunDate = message.getProcessing() == null ? null : message.getProcessing().getRunDate();
        this.runDate = messageRunDate == null ? null : messageRunDate.toString();
        return this;
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        MessageProcessingResult other = (MessageProcessingResult) obj;
        return restricted == other.restricted
                && Objects.equals(messageId, other.messageId)
                && Objects.equals(datasetPartitionValue, other.datasetPartitionValue)
                && Objects.equals(summaryRow, other.summaryRow)
                && Objects.equals(detailRow, other.detailRow)
                && Objects.equals(featureHitSummaryRow, other.featureHitSummaryRow)
                && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(sourceName, other.sourceName)
                && Objects.equals(sentDate, other.sentDate)
                && Objects.equals(runDate, other.runDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, restricted, datasetPartitionValue, summaryRow, detailRow,
                featureHitSummaryRow, errorMessage, sourceName, sentDate, runDate);
    }

    @Override
    public String toString() {
        return "MessageProcessingResult[messageId=" + messageId + ", restricted=" + restricted
                + ", datasetPartitionValue=" + datasetPartitionValue + ", summaryRow=" + summaryRow
                + ", detailRow=" + detailRow + ", featureHitSummaryRow=" + featureHitSummaryRow
                + ", errorMessage=" + errorMessage + ", sourceName=" + sourceName + ", sentDate=" + sentDate
                + ", runDate=" + runDate + "]";
    }
}
