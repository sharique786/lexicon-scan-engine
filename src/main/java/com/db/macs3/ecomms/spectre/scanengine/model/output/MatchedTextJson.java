package com.db.macs3.ecomms.spectre.scanengine.model.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The {@code matched_text} JSON value for ONE {@code term_dtls} entry of
 * {@code lexicon-hit-restricted}/{@code -unrestricted} — matches requirement
 * 2.i's {@code hit_details_hs} structure:
 * <pre>
 * {"hit_details_hs":[{
 *   "message_id": "...",
 *   "msg_text": [{"text": "bomb", "start": 10, "length": 4}, ...],
 *   "subject": {"text": "bomb", "start": 10, "length": 4},
 *   "attachment_text": [{"attachment_id": "...", "att_text": [...]}]
 * }]}
 * </pre>
 *
 * <p><b>One deliberate deviation from the literal example:</b> the example
 * shows {@code subject} as a single object, but a term can in principle
 * match more than once within a subject line just as it can within the
 * message body — {@link HitDetail#subject} is therefore a LIST here (empty
 * when there is no subject match, one entry per occurrence otherwise), for
 * the same reason {@code msg_text}/{@code att_text} are lists. Flagged
 * explicitly since it is a structural choice beyond what the example
 * literally specifies, not a re-derivation of something already unambiguous.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class MatchedTextJson implements Serializable {

    private final List<HitDetail> hitDetailsHs;

    @JsonCreator
    public MatchedTextJson(@JsonProperty("hit_details_hs") List<HitDetail> hitDetailsHs) {
        this.hitDetailsHs = hitDetailsHs;
    }

    @JsonProperty("hit_details_hs")
    public List<HitDetail> hitDetailsHs() { return hitDetailsHs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MatchedTextJson)) return false;
        return Objects.equals(hitDetailsHs, ((MatchedTextJson) o).hitDetailsHs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hitDetailsHs);
    }

    @Override
    public String toString() {
        return "MatchedTextJson[hitDetailsHs=" + hitDetailsHs + "]";
    }

    public static final class HitDetail implements Serializable {

        private final String messageId;
        private final List<TextHit> msgText;
        private final List<TextHit> subject;
        private final List<AttachmentTextHit> attachmentText;

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
        public String messageId() { return messageId; }
        @JsonProperty("msg_text")
        public List<TextHit> msgText() { return msgText; }
        @JsonProperty("subject")
        public List<TextHit> subject() { return subject; }
        @JsonProperty("attachment_text")
        public List<AttachmentTextHit> attachmentText() { return attachmentText; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HitDetail)) return false;
            HitDetail other = (HitDetail) o;
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

    public static final class TextHit implements Serializable {

        private final String text;
        private final int start;
        private final int length;

        /**
         * @param text      the matched text (stripped-text form — see {@code HtmlStrippingService})
         * @param start      original-text start character index
         * @param length     original-text span length ({@code end - start})
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
        public String text() { return text; }
        @JsonProperty("start")
        public int start() { return start; }
        @JsonProperty("length")
        public int length() { return length; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TextHit)) return false;
            TextHit other = (TextHit) o;
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

    public static final class AttachmentTextHit implements Serializable {

        private final String attachmentId;
        private final List<TextHit> attText;

        @JsonCreator
        public AttachmentTextHit(@JsonProperty("attachment_id") String attachmentId,
                                  @JsonProperty("att_text") List<TextHit> attText) {
            this.attachmentId = attachmentId;
            this.attText = attText;
        }

        @JsonProperty("attachment_id")
        public String attachmentId() { return attachmentId; }
        @JsonProperty("att_text")
        public List<TextHit> attText() { return attText; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AttachmentTextHit)) return false;
            AttachmentTextHit other = (AttachmentTextHit) o;
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
