package com.taosdata.flink.cdc.enumerator;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TDengineCdcEnumerator implements SplitEnumerator<TDengineCdcSplit, TDengineCdcEnumState> {
    private final SplitEnumeratorContext<TDengineCdcSplit> context;
    private final Boundedness boundedness;

    private Deque<TDengineCdcSplit> unassignedCdcSplits;
    private List<TDengineCdcSplit> assignmentCdcSplits;
    private final int readerCount;
    private int taskCount = 1;
    private String topic;
    private Properties properties;
    private boolean isInitFinished = false;

    public TDengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic, Properties properties) {
        this.context = context;
        this.boundedness = boundedness;
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        this.properties = properties;
        assignmentCdcSplits = new ArrayList<>();
    }

    public TDengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic, Properties properties, TDengineCdcEnumState checkpoint) {
        this.context = context;
        this.boundedness = boundedness;
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        this.properties = properties;
        assignmentCdcSplits = new ArrayList<>();
        if (checkpoint != null && checkpoint.isInitFinished()) {
            unassignedCdcSplits = checkpoint.getUnassignedCdcSplits();
            this.isInitFinished = true;
        }

    }
    @Override
    public void start() {
        if (!this.isInitFinished) {
            String groupId = properties.getProperty("group.id");
            unassignedCdcSplits = new ArrayDeque<>(readerCount);
            for (int i = 0; i < readerCount; i++) {
                TDengineCdcSplit cdcSplit = new TDengineCdcSplit(topic, groupId, "clientId_" + i,  (List<CdcTopicPartition>)null);
                unassignedCdcSplits.add(cdcSplit);
            }
            isInitFinished = true;
        }
    }


    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int i = 0;
    }

    @Override
    public void addSplitsBack(List<TDengineCdcSplit> splits, int subtaskId) {
        TreeSet<TDengineCdcSplit> splitSet = new TreeSet<>(unassignedCdcSplits);
        splitSet.addAll(splits);
        unassignedCdcSplits.addAll(splitSet);
        if (context.registeredReaders().containsKey(subtaskId)) {
            addReader(subtaskId);
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        checkReaderRegistered(subtaskId);
        if (!unassignedCdcSplits.isEmpty()) {
            TDengineCdcSplit cdcSplit = unassignedCdcSplits.pop();
            assignmentCdcSplits.add(cdcSplit);
            context.assignSplit(cdcSplit, subtaskId);
        }

        if (unassignedCdcSplits.isEmpty()) {
            Set<Integer> taskIds = context.registeredReaders().keySet();
            if (taskIds != null && taskIds.size() > 0) {
                for (Integer taskId : taskIds) {
                    context.signalNoMoreSplits(taskId);
                }
            }
        }

    }

    @Override
    public TDengineCdcEnumState snapshotState(long checkpointId) throws Exception {
        return new TDengineCdcEnumState(this.unassignedCdcSplits, this.assignmentCdcSplits, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {

    }

}
