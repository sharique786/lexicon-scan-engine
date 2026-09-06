package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Flattened, Spark-serialisable representation of one AVRO message record.
 * Only the fields this engine actually reads are carried;
 * {@code message.metadata} is intentionally NOT modelled here since nothing
 * in this engine's processing needs it.
 *
 * <h2>Which text field is scanned</h2>
 * <p>{@link MessageContent} carries both {@code rawText} and {@code cleanText}.
 * This engine scans {@code rawText} (via {@code HtmlStrippingService} — see
 * that class for when/why HTML stripping applies) rather than
 * {@code cleanText}, because {@code rawText} is the ORIGINAL text whose
 * character positions a match's {@code startCharIndex}/{@code endCharIndex}
 * must be reported against. {@code cleanText} is carried through for
 * completeness/parity with the source schema but is not itself scanned by
 * this engine.
 */
public class ScanMessage implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String messageId;
    private MessageSource source;
    private MessageContent content;
    private List<MessageAttachment> attachments;
    private MessageProcessing processing;
    private String datasetId;
    private boolean restricted;

    /**
     * @param messageId    joins to {@code FeatureDecisionRow.getMessageId}
     * @param source        channel/source-system identification
     * @param content       the message body — header, subject, raw/clean text
     * @param attachments   zero or more attached files' extracted text
     * @param processing    {@code run_date}/{@code run_hour} — used to resolve which
     *                       AVRO partition this message was read from
     * @param datasetId              which Airflow-supplied dataset this message came from —
     *                                 populated by the reader, not present in the AVRO itself
     * @param restricted             true if this message was read from a {@code restricted/}
     *                                 GCS subfolder, false if {@code unrestricted/} — populated
     *                                 by the reader from the source file path, not present in
     *                                 the AVRO itself; determines which output table
     *                                 (lexicon-hit-restricted vs -unrestricted) this message's
     *                                 hits are written to
     */
    public ScanMessage(String messageId, MessageSource source, MessageContent content,
                        List<MessageAttachment> attachments, MessageProcessing processing,
                        String datasetId, boolean restricted) {
        this.messageId = messageId;
        this.source = source;
        this.content = content;
        this.attachments = attachments;
        this.processing = processing;
        this.datasetId = datasetId;
        this.restricted = restricted;
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public MessageSource getSource() { return source; }
    public void setSource(MessageSource source) { this.source = source; }
    public MessageContent getContent() { return content; }
    public void setContent(MessageContent content) { this.content = content; }
    public List<MessageAttachment> getAttachments() { return attachments; }
    public void setAttachments(List<MessageAttachment> attachments) { this.attachments = attachments; }
    public MessageProcessing getProcessing() { return processing; }
    public void setProcessing(MessageProcessing processing) { this.processing = processing; }
    public String getDatasetId() { return datasetId; }
    public void setDatasetId(String datasetId) { this.datasetId = datasetId; }
    public boolean isRestricted() { return restricted; }
    public void setRestricted(boolean restricted) { this.restricted = restricted; }

    /** @return {@link #getAttachments}, or an empty list if the AVRO record had no attachments field/a null one. */
    public List<MessageAttachment> attachmentsOrEmpty() {
        return attachments == null ? List.of() : attachments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScanMessage)) {
            return false;
        }
        ScanMessage other = (ScanMessage) o;
        return restricted == other.restricted
                && Objects.equals(messageId, other.messageId)
                && Objects.equals(source, other.source)
                && Objects.equals(content, other.content)
                && Objects.equals(attachments, other.attachments)
                && Objects.equals(processing, other.processing)
                && Objects.equals(datasetId, other.datasetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, source, content, attachments, processing, datasetId, restricted);
    }

    @Override
    public String toString() {
        return "ScanMessage[messageId=" + messageId + ", source=" + source + ", content=" + content
                + ", attachments=" + attachments + ", processing=" + processing
                + ", datasetId=" + datasetId + ", restricted=" + restricted + "]";
    }
}
