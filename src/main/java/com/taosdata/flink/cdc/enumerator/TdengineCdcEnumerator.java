package com.taosdata.flink.cdc.enumerator;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TdengineCdcEnumerator implements SplitEnumerator<TDengineCdcSplit, TdengineCdcEnumState> {
    private final SplitEnumeratorContext<TDengineCdcSplit> context;
    private final Boundedness boundedness;

    private Deque<TDengineCdcSplit> unassignedCdcSplits;
    private List<TDengineCdcSplit> assignmentCdcSplits;
    private final int readerCount;
    private int taskCount = 1;
    private String topic;
    private boolean isInitFinished = false;

    public TdengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic) {
        this.context = context;
        this.boundedness = boundedness;
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        assignmentCdcSplits = new ArrayList<>();
    }

    public TdengineCdcEnumerator(SplitEnumeratorContext<TDengineCdcSplit> context,
                                 Boundedness boundedness, String topic, TdengineCdcEnumState checkpoint) {
        this.context = context;
        this.boundedness = boundedness;
        this.readerCount = context.currentParallelism();
        this.topic = topic;
        if (checkpoint != null && checkpoint.isInitFinished()) {
            assignmentCdcSplits = checkpoint.getAssignmentCdcSplits();
            unassignedCdcSplits = checkpoint.getUnassignedCdcSplits();
            this.isInitFinished = true;
        }

    }
    @Override
    public void start() {
        if (!this.isInitFinished) {
            unassignedCdcSplits = new ArrayDeque<>(readerCount);
            for (int i = 0; i < readerCount; i++) {
                TDengineCdcSplit cdcSplit = new TDengineCdcSplit(topic, "groupId_" + i, "clientId_" + i, null);
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
    public TdengineCdcEnumState snapshotState(long checkpointId) throws Exception {
        return new TdengineCdcEnumState(this.unassignedCdcSplits, this.assignmentCdcSplits, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {

    }

}
