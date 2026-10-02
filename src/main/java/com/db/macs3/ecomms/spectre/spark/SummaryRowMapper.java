package com.db.macs3.ecomms.spectre.spark;

import com.db.macs3.ecomms.spectre.bq.OutputTableWriter;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Maps each successful {@link MessageProcessingResult} in a partition to its
 * {@code lexicon-hit-summary} {@link Row}, skipping errored results — see
 * {@link ScanEngineJobRunner writeOutputs}, which runs this via
 * {@code Dataset.mapPartitions} directly on the cached results
 * {@code Dataset}, rather than via {@code JavaRDD.map}.
 *
 * <p>Stateless — holds no fields, so serialization (required since this runs
 * on executors) is trivial.
 */
public final class SummaryRowMapper implements MapPartitionsFunction<MessageProcessingResult, Row> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Row> call(Iterator<MessageProcessingResult> input) {
        List<Row> rows = new ArrayList<>();
        while (input.hasNext()) {
            MessageProcessingResult result = input.next();
            if (!result.isError()) {
                rows.add(OutputTableWriter.toRow(result.getSummaryRow()));
            }
        }
        return rows.iterator();
    }
}
