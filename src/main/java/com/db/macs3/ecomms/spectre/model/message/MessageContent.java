package com.db.macs3.ecomms.spectre.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * {@code message.content} block of the AVRO message schema.
 */
public class MessageContent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String header;
    private String rawText;
    private String subject;
    private String cleanText;

    /**
     * @param header    display header text, not scanned by this engine
     * @param rawText   the message body, in its ORIGINAL form (may contain HTML) —
     *                  the field this engine scans; see {@link ScanMessage} class Javadoc
     * @param subject   the message subject — scanned when a feature's scope includes it
     *                  (see {@code BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT})
     * @param cleanText a pre-cleaned form of the body, carried through for parity with
     *                  the source schema but not itself scanned — see {@link ScanMessage}
     */
    public MessageContent(String header, String rawText, String subject, String cleanText) {
        this.header = header;
        this.rawText = rawText;
        this.subject = subject;
        this.cleanText = cleanText;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getRawText() {
        return rawText;
    }

    public void setRawText(String rawText) {
        this.rawText = rawText;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getCleanText() {
        return cleanText;
    }

    public void setCleanText(String cleanText) {
        this.cleanText = cleanText;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        MessageContent other = (MessageContent) obj;
        return Objects.equals(header, other.header)
                && Objects.equals(rawText, other.rawText)
                && Objects.equals(subject, other.subject)
                && Objects.equals(cleanText, other.cleanText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, rawText, subject, cleanText);
    }

    @Override
    public String toString() {
        return "MessageContent[header=" + header + ", rawText=" + rawText
                + ", subject=" + subject + ", cleanText=" + cleanText + "]";
    }
}
