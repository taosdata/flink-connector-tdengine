package com.taosdata.flink.cdc.enumerator;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TDengineCdcEnumerator implements SplitEnumerator<TDengineCdcSplit, TDengineCdcEnumState> {
    private final Logger LOG = LoggerFactory.getLogger(TDengineCdcEnumerator.class);
    private final SplitEnumeratorContext<TDengineCdcSplit> context;
    private final Boundedness boundedness;

    private Deque<TDengineCdcSplit> unassignedCdcSplits;
    private List<TDengineCdcSplit> assignmentCdcSplits;
    private final int readerCount;
    private int taskCount = 1;
    private String topic;
    private Properties properties;
    private boolean isInitFinished = false;

    private HashSet<Integer> taskIdSet;

    public TDengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic, Properties properties) {
        this.context = context;
        this.boundedness = boundedness;
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        this.properties = properties;
        assignmentCdcSplits = new ArrayList<>();
        this.taskIdSet = new HashSet<>();
        LOG.info("create TDengineCdcEnumerator object, topic:{}, readerCount:{}", topic, readerCount);
    }

    public TDengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic, Properties properties, TDengineCdcEnumState checkpoint) {
        this.context = context;
        this.boundedness = boundedness;
        // get reader count
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        this.properties = properties;
        assignmentCdcSplits = new ArrayList<>();
        if (checkpoint != null && checkpoint.isInitFinished()) {
            unassignedCdcSplits = checkpoint.getUnassignedCdcSplits();
            this.isInitFinished = true;
        }
        this.taskIdSet = new HashSet<>();
        LOG.warn("restore TDengineCdcEnumerator object, topic:{}, readerCount:{}, isInitFinished:{}", topic, readerCount, this.isInitFinished);

    }

    /**
     * Create consumers based on parallelism for data extraction
     */
    @Override
    public void start() {
        if (!this.isInitFinished) {
            String groupId = properties.getProperty("group.id");
            unassignedCdcSplits = new ArrayDeque<>(readerCount);
            for (int i = 0; i < readerCount; i++) {
                TDengineCdcSplit cdcSplit = new TDengineCdcSplit(topic, groupId, "clientId_" + i,  (List<CdcTopicPartition>)null);
                unassignedCdcSplits.add(cdcSplit);
                LOG.debug("start assigned split，split.id:{}", cdcSplit.splitId());
            }
            isInitFinished = true;
        }
    }


    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("handleSplitRequest subtaskId:{}, requesterHostname:{}", subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<TDengineCdcSplit> splits, int subtaskId) {
        LOG.warn("addSplitsBack subtaskId:{}", splits);
        if (!splits.isEmpty()) {
            TreeSet<TDengineCdcSplit> splitSet = new TreeSet<>(unassignedCdcSplits);
            splitSet.addAll(splits);
            unassignedCdcSplits.addAll(splitSet);
            if (context.registeredReaders().containsKey(subtaskId)) {
                addReader(subtaskId);
            }
        }
        taskIdSet.add(subtaskId);
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.trace("addReader subtaskId:{}", subtaskId);
        checkReaderRegistered(subtaskId);
        if (!taskIdSet.contains(subtaskId)) {
            if (!unassignedCdcSplits.isEmpty()) {
                TDengineCdcSplit cdcSplit = unassignedCdcSplits.pop();
                assignmentCdcSplits.add(cdcSplit);
                context.assignSplit(cdcSplit, subtaskId);
                LOG.debug("addReader assigned subtaskId:{}, splitId:{}", subtaskId, cdcSplit.splitId());
            }
            taskIdSet.add(subtaskId);
        }

        if (unassignedCdcSplits.isEmpty()) {
            LOG.debug("Task assigned completed！");
            context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
        }
    }

    @Override
    public TDengineCdcEnumState snapshotState(long checkpointId) throws Exception {
        LOG.debug("split enumerator snapshotState, checkpointId:{}, unassigned:{}, assignment:{}",
                checkpointId, unassignedCdcSplits.size(), assignmentCdcSplits.size());
        return new TDengineCdcEnumState(this.unassignedCdcSplits, this.assignmentCdcSplits, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("split enumerator closed!");
    }

}
