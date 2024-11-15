package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceRecord;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.function.Consumer;

public class TDengineCdcSplitReader<T> implements SplitReader<Consumer<T>, TdengineSplit> {
    @Override
    public RecordsWithSplitIds<Consumer<T>> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TdengineSplit> splitsChanges) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
