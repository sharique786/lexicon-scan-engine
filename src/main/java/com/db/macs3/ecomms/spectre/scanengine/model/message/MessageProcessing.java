package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * {@code processing} block of the AVRO message schema — identifies which
 * partition (by run date/hour) this message record was written under.
 */
public class MessageProcessing implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String runDate;
    private String runHour;

    /**
     * @param runDate    {@code YYYY-MM-DD}-shaped partition date
     * @param runHour     partition hour
     */
    public MessageProcessing(String runDate, String runHour) {
        this.runDate = runDate;
        this.runHour = runHour;
    }

    public String getRunDate() { return runDate; }
    public void setRunDate(String runDate) { this.runDate = runDate; }
    public String getRunHour() { return runHour; }
    public void setRunHour(String runHour) { this.runHour = runHour; }

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
