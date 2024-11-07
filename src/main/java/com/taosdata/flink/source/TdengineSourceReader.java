package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.flink.source.entity.SourceRecords;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

public class TdengineSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<SourceRecords, T, TdengineSplit, TdengineSplitsState> {

    public TdengineSourceReader(SingleThreadFetcherManager splitFetcherManager,
                                RecordEmitter recordEmitter,
                                Configuration config,
                                SourceReaderContext readerContext) {

        super(splitFetcherManager, recordEmitter, config, readerContext);

    }

    @Override
    protected void onSplitFinished(Map map) {

    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    protected TdengineSplitsState initializedState(TdengineSplit tdengineSplit) {
        return null;
    }

    @Override
    protected TdengineSplit toSplitType(String s, TdengineSplitsState tdengineSplitsState) {
        return null;
    }
}
