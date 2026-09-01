package com.db.macs3.ecomms.spectre.scanengine.hyperscan;

/**
 * Thrown when a Hyperscan {@code .hdb} file's GCS stream fails to open/read,
 * or {@code Database.load} rejects its bytes (a corrupted file). This is a
 * job-level failure (unlike a single message's processing error), since a
 * database that fails to load cannot be scanned against by any message that
 * needs it.
 */
public final class HyperscanFileLoadException extends RuntimeException {
    public HyperscanFileLoadException(String message) {
        super(message);
    }

    public HyperscanFileLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
