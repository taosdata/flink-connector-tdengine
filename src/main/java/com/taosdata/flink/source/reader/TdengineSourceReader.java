package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.entity.SourceRecords;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import java.util.Map;

public class TdengineSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<SourceRecords, T, TDengineSplit, TDengineSplit> {

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
    protected TDengineSplit initializedState(TDengineSplit tdengineSplit) {
        return null;
    }

    @Override
    protected TDengineSplit toSplitType(String s, TDengineSplit TDengineSplitsState) {
        return null;
    }
}
