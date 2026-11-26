package com.db.macs3.ecomms.spectre.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * {@code message.metadata} block of the AVRO message schema. Only
 * {@code start_time_utc} is modelled — it is the source of
 * {@code pipeline_record_audit.sent_date}.
 */
public class MessageMetadata implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Instant startTimeUtc;

    /**
     * @param startTimeUtc {@code message.metadata.start_time_utc}; null when absent or not parseable
     *                     as a timestamp — see {@code RowReaders#getUtcInstantOrNull}
     */
    public MessageMetadata(Instant startTimeUtc) {
        this.startTimeUtc = startTimeUtc;
    }

    public Instant getStartTimeUtc() {
        return startTimeUtc;
    }

    public void setStartTimeUtc(Instant startTimeUtc) {
        this.startTimeUtc = startTimeUtc;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(startTimeUtc, ((MessageMetadata) obj).startTimeUtc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeUtc);
    }

    @Override
    public String toString() {
        return "MessageMetadata[startTimeUtc=" + startTimeUtc + "]";
    }
}
