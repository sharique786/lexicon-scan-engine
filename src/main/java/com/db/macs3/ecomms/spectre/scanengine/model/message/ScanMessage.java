package com.db.macs3.ecomms.spectre.scanengine.model.message;

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
public final class ScanMessage implements Serializable {

    private final String messageId;
    private final MessageSource source;
    private final MessageContent content;
    private final List<MessageAttachment> attachments;
    private final MessageProcessing processing;
    private final String datasetId;
    private final boolean restricted;

    /**
     * @param messageId    joins to {@code FeatureDecisionRow.messageId}
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

    public String messageId() { return messageId; }
    public MessageSource source() { return source; }
    public MessageContent content() { return content; }
    public List<MessageAttachment> attachments() { return attachments; }
    public MessageProcessing processing() { return processing; }
    public String datasetId() { return datasetId; }
    public boolean restricted() { return restricted; }

    /** @return {@link #attachments}, or an empty list if the AVRO record had no attachments field/a null one. */
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
