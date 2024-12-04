package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import java.util.Map;

public class TdengineSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<SourceRecord, T, TDengineSplit, TDengineSplitsState> {

    public TdengineSourceReader(SingleThreadFetcherManager splitFetcherManager,
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

}
