package com.db.macs3.ecomms.spectre.model.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The {@code matched_text} JSON value for ONE {@code term_dtls} entry of
 * {@code lexicon-hit-restricted}/{@code -unrestricted}:
 * <pre>
 * {"hit_details_hs":[{
 *   "message_id": "...",
 *   "msg_text": [{"text": "bomb", "start": 10, "length": 4}, ...],
 *   "subject": [{"text": "bomb", "start": 10, "length": 4}],
 *   "attachment_text": [{"attachment_id": "...", "att_text": [...]}]
 * }]}
 * </pre>
 *
 * <p>{@link HitDetail#getSubject} is a LIST — a term can match more than once
 * within a subject line just as it can within the message body — empty when
 * there is no subject match, one entry per occurrence otherwise, the same
 * shape as {@code msg_text}/{@code att_text}.
 *
 * <p>Every getter here still carries an explicit {@code @JsonProperty} even
 * though the {@code getXxx}/snake_case pairing would mostly auto-resolve —
 * this class is the ONLY one in this project actually round-tripped through
 * Jackson serialization (every sibling model class is only ever read
 * field-by-field by {@code OutputTableWriter}), so the JSON key mapping is
 * pinned explicitly rather than left to convention.
 */
public class MatchedTextJson implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private List<HitDetail> hitDetailsHs;

    @JsonCreator
    public MatchedTextJson(@JsonProperty("hit_details_hs") List<HitDetail> hitDetailsHs) {
        this.hitDetailsHs = hitDetailsHs;
    }

    @JsonProperty("hit_details_hs")
    public List<HitDetail> getHitDetailsHs() {
        return hitDetailsHs;
    }

    public void setHitDetailsHs(List<HitDetail> hitDetailsHs) {
        this.hitDetailsHs = hitDetailsHs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MatchedTextJson)) {
            return false;
        }
        return Objects.equals(hitDetailsHs, ((MatchedTextJson) obj).hitDetailsHs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hitDetailsHs);
    }

    @Override
    public String toString() {
        return "MatchedTextJson[hitDetailsHs=" + hitDetailsHs + "]";
    }

    public static class HitDetail implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String messageId;
        private List<TextHit> msgText;
        private List<TextHit> subject;
        private List<AttachmentTextHit> attachmentText;

        @JsonCreator
        public HitDetail(@JsonProperty("message_id") String messageId,
                         @JsonProperty("msg_text") List<TextHit> msgText,
                         @JsonProperty("subject") List<TextHit> subject,
                         @JsonProperty("attachment_text") List<AttachmentTextHit> attachmentText) {
            this.messageId = messageId;
            this.msgText = msgText;
            this.subject = subject;
            this.attachmentText = attachmentText;
        }

        @JsonProperty("message_id")
        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        @JsonProperty("msg_text")
        public List<TextHit> getMsgText() {
            return msgText;
        }

        public void setMsgText(List<TextHit> msgText) {
            this.msgText = msgText;
        }

        @JsonProperty("subject")
        public List<TextHit> getSubject() {
            return subject;
        }

        public void setSubject(List<TextHit> subject) {
            this.subject = subject;
        }

        @JsonProperty("attachment_text")
        public List<AttachmentTextHit> getAttachmentText() {
            return attachmentText;
        }

        public void setAttachmentText(List<AttachmentTextHit> attachmentText) {
            this.attachmentText = attachmentText;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof HitDetail)) {
                return false;
            }
            HitDetail other = (HitDetail) obj;
            return Objects.equals(messageId, other.messageId)
                    && Objects.equals(msgText, other.msgText)
                    && Objects.equals(subject, other.subject)
                    && Objects.equals(attachmentText, other.attachmentText);
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageId, msgText, subject, attachmentText);
        }

        @Override
        public String toString() {
            return "HitDetail[messageId=" + messageId + ", msgText=" + msgText
                    + ", subject=" + subject + ", attachmentText=" + attachmentText + "]";
        }
    }

    public static class TextHit implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String text;
        private int start;
        private int length;

        /**
         * @param text   the matched text (stripped-text form — see {@code HtmlStrippingService})
         * @param start  original-text start character index
         * @param length original-text span length ({@code end - start})
         */
        @JsonCreator
        public TextHit(@JsonProperty("text") String text,
                       @JsonProperty("start") int start,
                       @JsonProperty("length") int length) {
            this.text = text;
            this.start = start;
            this.length = length;
        }

        @JsonProperty("text")
        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        @JsonProperty("start")
        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        @JsonProperty("length")
        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TextHit)) {
                return false;
            }
            TextHit other = (TextHit) obj;
            return start == other.start && length == other.length && Objects.equals(text, other.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text, start, length);
        }

        @Override
        public String toString() {
            return "TextHit[text=" + text + ", start=" + start + ", length=" + length + "]";
        }
    }

    public static class AttachmentTextHit implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String attachmentId;
        private List<TextHit> attText;

        @JsonCreator
        public AttachmentTextHit(@JsonProperty("attachment_id") String attachmentId,
                                 @JsonProperty("att_text") List<TextHit> attText) {
            this.attachmentId = attachmentId;
            this.attText = attText;
        }

        @JsonProperty("attachment_id")
        public String getAttachmentId() {
            return attachmentId;
        }

        public void setAttachmentId(String attachmentId) {
            this.attachmentId = attachmentId;
        }

        @JsonProperty("att_text")
        public List<TextHit> getAttText() {
            return attText;
        }

        public void setAttText(List<TextHit> attText) {
            this.attText = attText;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AttachmentTextHit)) {
                return false;
            }
            AttachmentTextHit other = (AttachmentTextHit) obj;
            return Objects.equals(attachmentId, other.attachmentId) && Objects.equals(attText, other.attText);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attachmentId, attText);
        }

        @Override
        public String toString() {
            return "AttachmentTextHit[attachmentId=" + attachmentId + ", attText=" + attText + "]";
        }
    }
}
