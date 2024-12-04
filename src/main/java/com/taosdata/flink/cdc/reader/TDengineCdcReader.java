package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitState;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TDengineCdcReader<T> extends SingleThreadMultiplexSourceReaderBase<T, T, TDengineCdcSplit, TDengineCdcSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineCdcReader.class);

    private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit;


    public TDengineCdcReader(SingleThreadFetcherManager splitFetcherManager,
                             RecordEmitter recordEmitter,
                             Configuration config,
                             SourceReaderContext readerContext) {

        super(splitFetcherManager, recordEmitter, config, readerContext);
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
    }

    @Override
    protected void onSplitFinished(Map map) {
        int i = 0;
    }

    @Override
    protected TDengineCdcSplitState initializedState(TDengineCdcSplit cdcSplit) {
        return new TDengineCdcSplitState(cdcSplit);
    }

    @Override
    protected TDengineCdcSplit toSplitType(String splitId, TDengineCdcSplitState splitState) {
        return splitState.toTDengineCdcSplit();
    }

    @Override
    public List<TDengineCdcSplit> snapshotState(long checkpointId) {
        List<TDengineCdcSplit> cdcSplits = super.snapshotState(checkpointId);

        if (cdcSplits.isEmpty()) {
            offsetsToCommit.put(checkpointId, Collections.emptyMap());
        } else {

            Map<TopicPartition, OffsetAndMetadata> offsetsMap =
                    offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
            // Put the offsets of the active splits.
            for (TDengineCdcSplit split : cdcSplits) {
                // If the checkpoint is triggered before the partition starting offsets
                // is retrieved, do not commit the offsets for those partitions.
                if (split.getStartPartitions().size() >= 0) {
                    for (CdcTopicPartition cdcTopicPartition : split.getStartPartitions()) {
                        offsetsMap.put(new TopicPartition(cdcTopicPartition.getTopic(), cdcTopicPartition.getvGroupId()),
                                new OffsetAndMetadata(cdcTopicPartition.getPartition()));
                    }
                }
            }
        }
        return cdcSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committedPartitions =
                offsetsToCommit.get(checkpointId);
        if (committedPartitions == null) {
            LOG.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
            return;
        }

        if (committedPartitions.isEmpty()) {
            LOG.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }
        ((TDengineCdcFetcherManager) splitFetcherManager).commitOffsets(committedPartitions);
        removeAllOffsetsToCommitUpToCheckpoint(checkpointId);


    }
    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
            offsetsToCommit.remove(offsetsToCommit.firstKey());
        }
    }
}
