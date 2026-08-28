package com.db.macs3.ecomms.spectre.scanengine.model.match;

import java.io.Serializable;

/** Which part of a message a {@link MatchSpan} was found in. */
public enum MatchArea implements Serializable {
    SUBJECT,
    MESSAGE_BODY,
    ATTACHMENT
}
