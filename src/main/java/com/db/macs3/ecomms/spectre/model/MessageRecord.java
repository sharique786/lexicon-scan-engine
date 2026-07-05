package com.db.macs3.ecomms.spectre.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a single communication message, whether loaded from AVRO files
 * on GCS (production) or from JSON fixture files (integration testing — see
 * {@link com.db.macs3.ecomms.spectre.reader.JsonMessageReader}).
 *
 * <p>AVRO / JSON schema fields mapped:
 * <ul>
 *   <li>{@code message_id}         → {@link #messageId}</li>
 *   <li>{@code source_type}        → {@link #sourceType} ("chat" | "email")</li>
 *   <li>{@code run_date}           → {@link #runDate}</li>
 *   <li>{@code content.raw_text}   → {@link #contentRawText}</li>
 *   <li>{@code attachment[*].raw_text} → {@link #attachmentTexts} (order preserved)</li>
 * </ul>
 */
public class MessageRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private String messageId;
    private String sourceType;
    private String runDate;
    private String contentRawText;
    private List<String> attachmentTexts;

    public MessageRecord() {
        this.attachmentTexts = new ArrayList<>();
    }

    public static MessageRecord of(String messageId, String sourceType, String runDate,
                                    String contentRawText, List<String> attachmentTexts) {
        MessageRecord rec = new MessageRecord();
        rec.messageId       = messageId;
        rec.sourceType      = sourceType;
        rec.runDate         = runDate;
        rec.contentRawText  = contentRawText;
        rec.attachmentTexts = attachmentTexts != null ? new ArrayList<>(attachmentTexts) : new ArrayList<>();
        return rec;
    }

    public boolean hasContent() {
        return contentRawText != null && !contentRawText.isBlank();
    }

    public int attachmentCount() {
        return attachmentTexts == null ? 0 : attachmentTexts.size();
    }

    public List<String> getAttachmentTextsView() {
        return Collections.unmodifiableList(attachmentTexts != null ? attachmentTexts : Collections.emptyList());
    }

    public String getMessageId()               { return messageId; }
    public void setMessageId(String v)         { this.messageId = v; }
    public String getSourceType()              { return sourceType; }
    public void setSourceType(String v)        { this.sourceType = v; }
    public String getRunDate()                 { return runDate; }
    public void setRunDate(String v)           { this.runDate = v; }
    public String getContentRawText()          { return contentRawText; }
    public void setContentRawText(String v)    { this.contentRawText = v; }
    public List<String> getAttachmentTexts()   { return attachmentTexts; }
    public void setAttachmentTexts(List<String> v) { this.attachmentTexts = v != null ? v : new ArrayList<>(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(messageId, ((MessageRecord) o).messageId);
    }

    @Override
    public int hashCode() { return Objects.hash(messageId); }

    @Override
    public String toString() {
        return "MessageRecord{messageId='" + messageId + "', sourceType='" + sourceType +
               "', attachments=" + attachmentCount() + '}';
    }
}
