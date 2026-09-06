package com.db.macs3.ecomms.spectre.scanengine.model.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/** {@code source} block of the AVRO message schema. */
public class MessageSource implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String channelName;
    private String sourceName;
    private String srcSysName;
    private String srcSysConvId;

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

    public String getChannelName() { return channelName; }
    public void setChannelName(String channelName) { this.channelName = channelName; }
    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }
    public String getSrcSysName() { return srcSysName; }
    public void setSrcSysName(String srcSysName) { this.srcSysName = srcSysName; }
    public String getSrcSysConvId() { return srcSysConvId; }
    public void setSrcSysConvId(String srcSysConvId) { this.srcSysConvId = srcSysConvId; }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MessageSource)) {
            return false;
        }
        MessageSource other = (MessageSource) obj;
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
