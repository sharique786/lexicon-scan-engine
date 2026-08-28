package com.db.macs3.ecomms.spectre.scanengine.model.match;

import java.io.Serializable;
import java.util.Objects;

/**
 * A {@link MatchSpan} tagged with which part of the message it came from —
 * needed because the output shape (see requirement 2.i's {@code hit_details_hs}
 * JSON) organises matches by area (subject / message body / per-attachment),
 * not just as one flat list.
 *
 * <p>Java 11 class (not a record — this project targets Java 11).
 */
public final class AreaMatch implements Serializable {

    private final MatchArea area;
    private final String attachmentId;
    private final MatchSpan span;

    /**
     * @param area            which part of the message this match is in
     * @param attachmentId    non-null iff {@code area == ATTACHMENT} — identifies WHICH
     *                          attachment (a message can have several)
     * @param span             the match's original-text position and text
     */
    public AreaMatch(MatchArea area, String attachmentId, MatchSpan span) {
        if (area == MatchArea.ATTACHMENT && (attachmentId == null || attachmentId.isBlank())) {
            throw new IllegalArgumentException("attachmentId is required when area == ATTACHMENT");
        }
        if (area != MatchArea.ATTACHMENT && attachmentId != null) {
            throw new IllegalArgumentException("attachmentId must be null when area != ATTACHMENT, got " + area);
        }
        this.area = area;
        this.attachmentId = attachmentId;
        this.span = span;
    }

    public MatchArea area() { return area; }
    public String attachmentId() { return attachmentId; }
    public MatchSpan span() { return span; }

    public static AreaMatch subject(MatchSpan span) {
        return new AreaMatch(MatchArea.SUBJECT, null, span);
    }

    public static AreaMatch messageBody(MatchSpan span) {
        return new AreaMatch(MatchArea.MESSAGE_BODY, null, span);
    }

    public static AreaMatch attachment(String attachmentId, MatchSpan span) {
        return new AreaMatch(MatchArea.ATTACHMENT, attachmentId, span);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AreaMatch)) return false;
        AreaMatch other = (AreaMatch) o;
        return area == other.area
                && Objects.equals(attachmentId, other.attachmentId)
                && Objects.equals(span, other.span);
    }

    @Override
    public int hashCode() {
        return Objects.hash(area, attachmentId, span);
    }

    @Override
    public String toString() {
        return "AreaMatch[area=" + area + ", attachmentId=" + attachmentId + ", span=" + span + "]";
    }
}
