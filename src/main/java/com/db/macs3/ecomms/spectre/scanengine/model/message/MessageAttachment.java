package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serializable;
import java.util.Objects;

/**
 * One entry of the AVRO message schema's {@code attachments} array.
 *
 * <p>Attachment content is exposed only as {@code clean_text} in the source
 * schema (no {@code raw_text} alternative) — it is treated as already free
 * of HTML by the upstream Msg Transformer job, so no HTML-stripping/offset-
 * mapping is applied when scanning it (contrast {@link MessageContent#rawText}).
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class MessageAttachment implements Serializable {

    private final String attachmentId;
    private final String parentAttachmentId;
    private final String fileName;
    private final String cleanText;

    /**
     * @param attachmentId          this attachment's identifier
     * @param parentAttachmentId    identifier of a parent attachment, when this one is
     *                                nested/derived from another (e.g. an extracted embedded file)
     * @param fileName                original filename
     * @param cleanText               extracted text content to scan — subject to the
     *                                {@code SPECTRE_MAX_ATTACHMENT_SIZE_BYTES} size limit,
     *                                see {@code ScanEngineProperties}
     */
    public MessageAttachment(String attachmentId, String parentAttachmentId, String fileName, String cleanText) {
        this.attachmentId = attachmentId;
        this.parentAttachmentId = parentAttachmentId;
        this.fileName = fileName;
        this.cleanText = cleanText;
    }

    public String attachmentId() { return attachmentId; }
    public String parentAttachmentId() { return parentAttachmentId; }
    public String fileName() { return fileName; }
    public String cleanText() { return cleanText; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageAttachment)) return false;
        MessageAttachment other = (MessageAttachment) o;
        return Objects.equals(attachmentId, other.attachmentId)
                && Objects.equals(parentAttachmentId, other.parentAttachmentId)
                && Objects.equals(fileName, other.fileName)
                && Objects.equals(cleanText, other.cleanText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attachmentId, parentAttachmentId, fileName, cleanText);
    }

    @Override
    public String toString() {
        return "MessageAttachment[attachmentId=" + attachmentId + ", parentAttachmentId=" + parentAttachmentId
                + ", fileName=" + fileName + ", cleanText=" + cleanText + "]";
    }
}
