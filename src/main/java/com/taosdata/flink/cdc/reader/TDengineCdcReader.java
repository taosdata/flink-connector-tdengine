package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitState;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TDengineCdcReader<T> extends SingleThreadMultiplexSourceReaderBase<T, T, TDengineCdcSplit, TDengineCdcSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineCdcReader.class);

    private final SortedMap<Long, List<CdcTopicPartition>> offsetsToCommit;

    private Map<Integer, Long> commitedOffsets;

    private final boolean autoCommit;

    private final TDengineCdcFetcherManager<T> cdcFetcherManager;

    /**
     * Cdc Reader: Used for reading data sources
     * @param elementsQueue The queue handing over data from the fetcher to the reader,
     *                      the same instance that was passed to the fetcher manager
     * @param cdcFetcherManager  Used for managing slice readers with CDC-specific offset commit support
     * @param recordEmitter Distribute data to downstream operators
     * @param config configuration parameter
     * @param readerContext Reader context information, mainly used to process reader status information
     * @param autoCommit If the downstream is a stateless operator,
     *                   automatic commit can be performed here,
     *                   otherwise set to false and rely on checkpoint for manual commit
     */
    @SuppressWarnings("deprecation")
    public TDengineCdcReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
                             TDengineCdcFetcherManager<T> cdcFetcherManager,
                             RecordEmitter recordEmitter,
                             Configuration config,
                             SourceReaderContext readerContext,
                             boolean autoCommit) {

        // The (elementsQueue, fetcherManager, ...) constructor is the only one that exists on
        // both Flink 1.15 and newer versions; the 4-argument variant was added in Flink 1.19.
        super(elementsQueue, cdcFetcherManager, recordEmitter, config, readerContext);
        this.cdcFetcherManager = cdcFetcherManager;
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.autoCommit = autoCommit;
        LOG.debug("TDengineCdcReader autoCommit is {}", this.autoCommit);
        commitedOffsets = new HashMap<>();

    }

    @Override
    protected void onSplitFinished(Map<String, TDengineCdcSplitState> finishedSplitIds) {
        if (finishedSplitIds != null && !finishedSplitIds.isEmpty()) {
            finishedSplitIds.forEach((key, state) -> {
                LOG.info("split reader {} has been completed!", state.splitId());
            });
        }
    }

    @Override
    protected TDengineCdcSplitState initializedState(TDengineCdcSplit cdcSplit) {
        LOG.debug("initializedState splitId:{}", cdcSplit.splitId());
        return new TDengineCdcSplitState(cdcSplit);
    }

    @Override
    protected TDengineCdcSplit toSplitType(String splitId, TDengineCdcSplitState splitState) {
        return splitState.toTDengineCdcSplit();
    }

    @Override
    public List<TDengineCdcSplit> snapshotState(long checkpointId) {
        LOG.trace("snapshotState checkpointId:{}", checkpointId);
        // At the beginning of the checkpoint, get current offset to be committed
        List<TDengineCdcSplit> cdcSplits = super.snapshotState(checkpointId);
        if (autoCommit) {
            return cdcSplits;
        }

        if (cdcSplits.isEmpty()) {
            LOG.debug("snapshotState splits is empty, checkpointId:{}", checkpointId);
            offsetsToCommit.put(checkpointId, Collections.emptyList());
        } else {
            // // save current offset to be committed
            List<CdcTopicPartition> offsetsList =
                    offsetsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
            // Put the offsets of the active splits.
            for (TDengineCdcSplit split : cdcSplits) {
                // If the checkpoint is triggered before the partition starting offsets
                // is retrieved, do not commit the offsets for those partitions.
                if (!split.getStartPartitions().isEmpty()) {
                    for (CdcTopicPartition cdcTopicPartition : split.getStartPartitions()) {
                        Long offset = commitedOffsets.get(cdcTopicPartition.hashCode());
                        // Filter the offset of submitted records，avoid duplicate submissions
                        if (offset == null || !offset.equals(cdcTopicPartition.getPartition()) ) {
                            LOG.debug("TDengineCdcReader fetches the snapshot state of a given topic and partition，info:{}", cdcTopicPartition.toString());
                            offsetsList.add(cdcTopicPartition);
                        }
                    }
                }
            }
        }
        return cdcSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.trace("notifyCheckpointComplete checkpointId:{}", checkpointId);
        if (autoCommit) {
            return;
        }

        List<CdcTopicPartition> cdcTopicPartitions = offsetsToCommit.get(checkpointId);

        if (cdcTopicPartitions == null || cdcTopicPartitions.isEmpty()) {
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            LOG.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> committedPartitions = new HashMap<>(cdcTopicPartitions.size());
        for (CdcTopicPartition cdcTopicPartition : cdcTopicPartitions) {
            committedPartitions.put(new TopicPartition(cdcTopicPartition.getTopic(), cdcTopicPartition.getvGroupId()),
                    new OffsetAndMetadata(cdcTopicPartition.getPartition()));
        }

        LOG.debug("notifyCheckpointComplete commitOffsets count:{}", committedPartitions.size());
        cdcFetcherManager.commitOffsets(committedPartitions);
        // clear checkpoint commit info
        removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
        // update vgroup offset, avoid duplicate submissions
        cdcTopicPartitions.forEach(value -> commitedOffsets.put(value.hashCode(), value.getPartition()));

    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!offsetsToCommit.isEmpty() && offsetsToCommit.firstKey() <= checkpointId) {
            offsetsToCommit.remove(offsetsToCommit.firstKey());
        }
    }
}
