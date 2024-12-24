package com.taosdata.flink.source.reader;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.source.entity.SplitResultRecord;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import java.util.List;
import java.util.Map;

public class TDengineSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<SplitResultRecord, T, TDengineSplit, TDengineSplitsState> {

    public TDengineSourceReader(SingleThreadFetcherManager splitFetcherManager,
                                RecordEmitter recordEmitter,
                                Configuration config,
                                SourceReaderContext readerContext) {

        super(splitFetcherManager, recordEmitter, config, readerContext);

    }

    @Override
    protected void onSplitFinished(Map map) {
        int i = 0;
    }

    @Override
    protected TDengineSplitsState initializedState(TDengineSplit tdengineSplit) {
        return new TDengineSplitsState(tdengineSplit);
    }

    @Override
    protected TDengineSplit toSplitType(String splitId, TDengineSplitsState splitState) {
        return splitState;
    }
    @Override
    public List<TDengineSplit> snapshotState(long checkpointId) {
        List<TDengineSplit> splits = super.snapshotState(checkpointId);
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        int i  = 0;
    }
}
