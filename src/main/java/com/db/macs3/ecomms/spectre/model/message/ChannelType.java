package com.db.macs3.ecomms.spectre.model.message;

/**
 * The message's originating channel — drives which HTML-handling strategy
 * {@code FeatureScanOrchestrator} applies to {@code content.raw_text} before
 * scanning it against Hyperscan. Resolved from {@link MessageSource#getChannelName()}
 * (the AVRO {@code source.channel_name} field), matched case-insensitively —
 * this project's AVRO does not carry a separate, distinctly-named
 * {@code channel_type} field, and {@code channel_name} is the one place a
 * value like {@code "EMAIL"}/{@code "CHAT"}/{@code "VOICE"} (or
 * {@code "email"}/{@code "chat"}/{@code "voice"}) is actually populated.
 *
 * <h2>Why EMAIL is the fallback, not a dedicated UNKNOWN</h2>
 * <p>{@code EMAIL} is this engine's original behaviour — strip
 * {@code raw_text} directly, with no {@code <td>}-cell extraction pass (see
 * {@code FeatureScanOrchestrator} class Javadoc). A null/blank/unrecognised
 * {@code channel_name} therefore falls back to that same original behaviour
 * rather than a separate branch, so a message with no channel information at
 * all (or one from a channel this engine doesn't yet special-case) is still
 * scanned, just without the CHAT/VOICE-specific pre-extraction.
 */
public enum ChannelType {
    EMAIL,
    CHAT,
    VOICE;

    /**
     * @param channelName {@code MessageSource#getChannelName()} — may be null/blank/unrecognised
     * @return the matching {@link ChannelType}, case-insensitively; {@link #EMAIL} otherwise — see class Javadoc
     */
    public static ChannelType fromChannelName(String channelName) {
        if (channelName == null) {
            return EMAIL;
        }
        String trimmed = channelName.trim();
        for (ChannelType type : values()) {
            if (type.name().equalsIgnoreCase(trimmed)) {
                return type;
            }
        }
        return EMAIL;
    }
}
