package com.db.macs3.ecomms.spectre.scanengine.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the Dataproc job's {@code --key=value} style positional arguments
 * (the shape Airflow/Composer now submits — see {@link RuntimeArgs} class
 * Javadoc) into a plain {@code Map<String, String>}, keyed by the part after
 * {@code --} and before the first {@code =}.
 *
 * <p>Each value is taken verbatim from the first {@code =} to the end of the
 * argument string — this matters for {@code --dataset_details=[{"dataset_id"
 * :"...","dataset_partition_value":"..."}]}, whose value is itself a JSON
 * array that may legitimately contain further characters (colons, braces,
 * quotes) but never another {@code --key=} boundary, so splitting once per
 * argument on the first {@code =} is unambiguous and safe.
 */
public final class CliArgumentParser {

    private static final String PREFIX = "--";

    private CliArgumentParser() {}

    public static Map<String, String> parse(String[] args) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String arg : args) {
            if (arg == null || !arg.startsWith(PREFIX)) {
                throw new IllegalArgumentException(
                        "Malformed argument (expected --key=value): " + arg);
            }
            int equalsIndex = arg.indexOf('=');
            if (equalsIndex < 0) {
                throw new IllegalArgumentException(
                        "Malformed argument (expected --key=value): " + arg);
            }
            String key = arg.substring(PREFIX.length(), equalsIndex);
            String value = arg.substring(equalsIndex + 1);
            result.put(key, value);
        }
        return result;
    }

    /** @throws IllegalArgumentException if {@code key} is missing or blank in {@code args} */
    public static String require(Map<String, String> args, String key) {
        String value = args.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: --" + key);
        }
        return value;
    }
}
