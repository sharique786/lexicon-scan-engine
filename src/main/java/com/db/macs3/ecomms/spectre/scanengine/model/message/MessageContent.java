package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serializable;
import java.util.Objects;

/** {@code message.content} block of the AVRO message schema. */
public final class MessageContent implements Serializable {

    private final String header;
    private final String rawText;
    private final String subject;
    private final String cleanText;

    /**
     * @param header      display header text, not scanned by this engine
     * @param rawText      the message body, in its ORIGINAL form (may contain HTML) —
     *                      the field this engine scans; see {@link ScanMessage} class Javadoc
     * @param subject      the message subject — scanned when a feature's scope includes it
     *                      (see {@code BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT})
     * @param cleanText    a pre-cleaned form of the body, carried through for parity with
     *                      the source schema but not itself scanned — see {@link ScanMessage}
     */
    public MessageContent(String header, String rawText, String subject, String cleanText) {
        this.header = header;
        this.rawText = rawText;
        this.subject = subject;
        this.cleanText = cleanText;
    }

    public String header() { return header; }
    public String rawText() { return rawText; }
    public String subject() { return subject; }
    public String cleanText() { return cleanText; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageContent)) {
            return false;
        }
        MessageContent other = (MessageContent) o;
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
