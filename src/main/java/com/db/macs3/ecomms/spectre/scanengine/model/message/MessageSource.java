package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@code source} block of the AVRO message schema.
 *
 * <p>Java 11 class (not a record — this project targets Java 11) exposing
 * the same accessor-method-per-field shape a record would, so every call
 * site reads identically to before.
 */
public final class MessageSource implements Serializable {

    private final String channelName;
    private final String sourceName;
    private final String srcSysName;
    private final String srcSysConvId;

    /**
     * @param channelName    {@code "chat"} / {@code "email"} / {@code "voice"}
     * @param sourceName      the originating system's display name
     * @param srcSysName      the originating system's identifier
     * @param srcSysConvId    the originating system's conversation/thread identifier
     */
    public MessageSource(String channelName, String sourceName, String srcSysName, String srcSysConvId) {
        this.channelName = channelName;
        this.sourceName = sourceName;
        this.srcSysName = srcSysName;
        this.srcSysConvId = srcSysConvId;
    }

    public String channelName() { return channelName; }
    public String sourceName() { return sourceName; }
    public String srcSysName() { return srcSysName; }
    public String srcSysConvId() { return srcSysConvId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageSource)) return false;
        MessageSource other = (MessageSource) o;
        return Objects.equals(channelName, other.channelName)
                && Objects.equals(sourceName, other.sourceName)
                && Objects.equals(srcSysName, other.srcSysName)
                && Objects.equals(srcSysConvId, other.srcSysConvId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, sourceName, srcSysName, srcSysConvId);
    }

    @Override
    public String toString() {
        return "MessageSource[channelName=" + channelName + ", sourceName=" + sourceName
                + ", srcSysName=" + srcSysName + ", srcSysConvId=" + srcSysConvId + "]";
    }
}
