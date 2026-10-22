package com.db.macs3.ecomms.spectre.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

/**
 * {@code processing} block of the AVRO message schema — identifies which
 * partition (by run date/hour) this message record was written under.
 */
public class MessageProcessing implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private LocalDate runDate;
    private String runHour;

    /**
     * @param runDate partition date — AVRO {@code date}-logical-type field
     * @param runHour partition hour
     */
    public MessageProcessing(LocalDate runDate, String runHour) {
        this.runDate = runDate;
        this.runHour = runHour;
    }

    public LocalDate getRunDate() {
        return runDate;
    }

    public void setRunDate(LocalDate runDate) {
        this.runDate = runDate;
    }

    public String getRunHour() {
        return runHour;
    }

    public void setRunHour(String runHour) {
        this.runHour = runHour;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        MessageProcessing other = (MessageProcessing) obj;
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
