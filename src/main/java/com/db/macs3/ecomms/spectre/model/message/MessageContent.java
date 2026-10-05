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
    private String msgText;
    private String subject;

    /**
     * @param header    display header text, not scanned by this engine
     * @param msgText   the message body, in its ORIGINAL form (may contain HTML) —
     *                  the field this engine scans; see {@link ScanMessage} class Javadoc
     * @param subject   the message subject — scanned when a feature's scope includes it
     *                  (see {@code BqColumns.FeatureDefinitionJson.SCOPE_SUBJECT})
     */
    public MessageContent(String header, String msgText, String subject) {
        this.header = header;
        this.msgText = msgText;
        this.subject = subject;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getMsgText() {
        return msgText;
    }

    public void setMsgText(String msgText) {
        this.msgText = msgText;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
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
                && Objects.equals(msgText, other.msgText)
                && Objects.equals(subject, other.subject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, msgText, subject);
    }

    @Override
    public String toString() {
        return "MessageContent[header=" + header + ", msgText=" + msgText
                + ", subject=" + subject + "]";
    }
}
