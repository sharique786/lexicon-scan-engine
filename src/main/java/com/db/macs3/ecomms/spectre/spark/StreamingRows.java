package com.db.macs3.ecomms.spectre.spark;

import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Lazily maps a partition's {@link MessageProcessingResult}s to output {@link Row}s, one at a time, instead of
 * buffering the whole partition in a list. The mapper function returns {@code null} to skip a result.
 */
final class StreamingRows {

    private StreamingRows() {
    }

    static Iterator<Row> map(Iterator<MessageProcessingResult> input, Function<MessageProcessingResult, Row> mapper) {
        return new Iterator<>() {
            private Row next;

            @Override
            public boolean hasNext() {
                while (next == null && input.hasNext()) {
                    next = mapper.apply(input.next());
                }
                return next != null;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Row row = next;
                next = null;
                return row;
            }
        };
    }
}
