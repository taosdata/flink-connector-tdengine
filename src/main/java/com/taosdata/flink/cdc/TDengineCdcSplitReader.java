package com.taosdata.flink.cdc;

import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.function.Consumer;

public class TDengineCdcSplitReader<T> implements SplitReader<Consumer<T>, TDengineSplit> {
    @Override
    public RecordsWithSplitIds<Consumer<T>> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TDengineSplit> splitsChanges) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
