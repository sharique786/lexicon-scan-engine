package com.db.macs3.ecomms.spectre.scanengine.avro;

/** Field names and path segments used when reading and converting AVRO message rows. */
final class AvroConstants {

    private AvroConstants() {}

    static final String FORMAT = "avro";
    static final String RESTRICTED_SUBFOLDER = "restricted/";
    static final String UNRESTRICTED_SUBFOLDER = "unrestricted/";
    static final String COLUMN_DATASET_ID = "dataset_id";
    static final String COLUMN_RESTRICTED = "restricted";

    static final String FIELD_MESSAGE_ID = "message_id";
    static final String FIELD_SOURCE = "source";
    static final String FIELD_CHANNEL_NAME = "channel_name";
    static final String FIELD_SOURCE_NAME = "source_name";
    static final String FIELD_SRC_SYS_NAME = "src_sys_name";
    static final String FIELD_SRC_SYS_CONV_ID = "src_sys_conv_id";
    static final String FIELD_MESSAGE = "message";
    static final String FIELD_CONTENT = "content";
    static final String FIELD_HEADER = "header";
    static final String FIELD_RAW_TEXT = "raw_text";
    static final String FIELD_SUBJECT = "subject";
    static final String FIELD_CLEAN_TEXT = "clean_text";
    static final String FIELD_ATTACHMENTS = "attachments";
    static final String FIELD_METADATA = "metadata";
    static final String FIELD_ATTACHMENT_ID = "attachment_id";
    static final String FIELD_PARENT_ATTACHMENT_ID = "parent_attachment_id";
    static final String FIELD_FILE_NAME = "file_name";
    static final String FIELD_PROCESSING = "processing";
    static final String FIELD_RUN_DATE = "run_date";
    static final String FIELD_RUN_HOUR = "run_hour";
}
