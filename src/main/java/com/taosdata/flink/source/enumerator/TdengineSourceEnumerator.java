package com.taosdata.flink.source.enumerator;

import com.taosdata.flink.source.TdengineSplit;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TdengineSourceEnumerator implements SplitEnumerator<TdengineSplit, TdengineSourceEnumState> {

    private final Deque<TdengineSplit> unassignedSqls;
    private final TreeSet<Integer> readersAwaitingSplit;
    private final Map<Integer, TdengineSplit> assignmentSqls;
    private final SplitEnumeratorContext<TdengineSplit> context;
    private final Boundedness boundedness;
    private final SourceSplitSql sourceSql;
    private final int readerCount;

    public TdengineSourceEnumerator (SplitEnumeratorContext<TdengineSplit> context,
                                     Boundedness boundedness, SourceSplitSql sourceSql) {
        this.readersAwaitingSplit = new TreeSet<>();
        this.assignmentSqls = new HashMap<>();
        this.unassignedSqls = new ArrayDeque<>();
        this.context = context;
        this.boundedness = boundedness;
        this.sourceSql = sourceSql;
        this.readerCount = context.currentParallelism();
    }

    @Override
    public void start() {
        if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TIMESTAMP) {
            TimestampSplitInfo timestampSplitInfo = sourceSql.getTimestampSplitInfo();
            if (timestampSplitInfo != null && timestampSplitInfo.getEndTime().after(timestampSplitInfo.getStartTime())) {
               long timeDifference = timestampSplitInfo.getEndTime().getTime() - timestampSplitInfo.getStartTime().getTime();
               long nCount = timeDifference / timestampSplitInfo.getInterval();
               if (nCount == 0) {
                   String sql = "select " + this.sourceSql.getSelect()
                           + " from `" + this.sourceSql.getTableName()
                           + "` where " + timestampSplitInfo.getFieldName() + " > " + timestampSplitInfo.getStartTime().getTime()
                           + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime().getTime()
                           + " and " + this.sourceSql.getWhere();

                   this.unassignedSqls.push(new TdengineSplit("0", sql));

               } else {
                   long startTime = timestampSplitInfo.getStartTime().getTime();
                   boolean bRemainder = timeDifference % timestampSplitInfo.getInterval() > 0;
                   for (int i = 0; i< nCount; i++) {
                       String sql = "select " + this.sourceSql.getSelect()
                               + " from `" + this.sourceSql.getTableName()
                               + "` where " + timestampSplitInfo.getFieldName() + " > " + startTime;

                       if (!bRemainder && i == nCount - 1) {
                           sql +=  " and " + timestampSplitInfo.getFieldName() + " < " + startTime + timestampSplitInfo.getInterval();
                       }else {
                           sql += " and " + timestampSplitInfo.getFieldName() + " <= " + startTime + timestampSplitInfo.getInterval();
                       }
                       sql += " and " + this.sourceSql.getWhere();
                       this.unassignedSqls.push(new TdengineSplit("" + i, sql));
                   }

                   if (bRemainder) {
                           String sql = "select " + this.sourceSql.getSelect()
                                   + " from `" + this.sourceSql.getTableName()
                                   + "` where " + timestampSplitInfo.getFieldName() + " > " + startTime
                                   + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime().getTime()
                                   + " and " + this.sourceSql.getWhere();
                           this.unassignedSqls.push(new TdengineSplit("" + nCount, sql));
                   }
               }
            }
        } else {
            String sql = "select " + this.sourceSql.getSelect()
                    + " from `" + this.sourceSql.getTableName() + "` ";
            if (!this.sourceSql.getWhere().isEmpty()) {
                sql += "where " + this.sourceSql.getWhere();
            }
            this.unassignedSqls.push(new TdengineSplit("0", sql));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int i = 0;
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }
    @Override
    public void addSplitsBack(List<TdengineSplit> list, int i) {
        int ii = 0;
    }

    @Override
    public void addReader(int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        checkReaderRegistered(subtaskId);
        if (!unassignedSqls.isEmpty()) {
            TdengineSplit split = unassignedSqls.pop();
            context.assignSplit(split, subtaskId);
            assignmentSqls.put(subtaskId, split);
        }else{
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public TdengineSourceEnumState snapshotState(long l) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        SplitEnumerator.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SplitEnumerator.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        SplitEnumerator.super.handleSourceEvent(subtaskId, sourceEvent);
    }
}
