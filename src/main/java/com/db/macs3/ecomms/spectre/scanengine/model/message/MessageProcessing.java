package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@code processing} block of the AVRO message schema — identifies which
 * partition (by run date/hour) this message record was written under.
 */
public final class MessageProcessing implements Serializable {

    private final String runDate;
    private final String runHour;

    /**
     * @param runDate    {@code YYYY-MM-DD}-shaped partition date
     * @param runHour     partition hour
     */
    public MessageProcessing(String runDate, String runHour) {
        this.runDate = runDate;
        this.runHour = runHour;
    }

    public String runDate() { return runDate; }
    public String runHour() { return runHour; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageProcessing)) {
            return false;
        }
        MessageProcessing other = (MessageProcessing) o;
        return Objects.equals(runDate, other.runDate) && Objects.equals(runHour, other.runHour);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runDate, runHour);
    }

    @Override
    public String toString() {
        return "MessageProcessing[runDate=" + runDate + ", runHour=" + runHour + "]";
    }
}
