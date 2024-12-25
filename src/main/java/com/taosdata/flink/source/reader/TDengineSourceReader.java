package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.entity.SplitResultRecord;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TDengineSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<SplitResultRecord, T, TDengineSplit, TDengineSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineSourceReader.class);
    public TDengineSourceReader(SingleThreadFetcherManager splitFetcherManager,
                                RecordEmitter recordEmitter,
                                Configuration config,
                                SourceReaderContext readerContext) {

        super(splitFetcherManager, recordEmitter, config, readerContext);

    }

    /**
     * @param finishedSplitIds
     */
    @Override
    protected void onSplitFinished(Map<String, TDengineSplitsState> finishedSplitIds) {
        finishedSplitIds.forEach((key, state) -> {
            if (state.getFinishList() != null && !state.getFinishList().isEmpty()) {
                state.getFinishList().forEach((task) -> {
                    LOG.info("Task {} of split reader {} has been completed!", key, task);
                });
            }else{
                LOG.info("split reader {} has been completed!", key);
            }
        });
    }

    @Override
    protected TDengineSplitsState initializedState(TDengineSplit tdengineSplit) {
        LOG.debug("initializedState splitId:{}", tdengineSplit.splitId());
        return new TDengineSplitsState(tdengineSplit);
    }

    @Override
    protected TDengineSplit toSplitType(String splitId, TDengineSplitsState splitState) {
        return splitState;
    }
    @Override
    public List<TDengineSplit> snapshotState(long checkpointId) {
        LOG.trace("snapshotState checkpointId:{}", checkpointId);
        return super.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.trace("notifyCheckpointComplete checkpointId:{}", checkpointId);
    }
}
