package com.taosdata.flink.source.enumerator;

import com.google.common.base.Strings;
import com.taosdata.flink.source.split.TdengineSplit;
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

    private final Deque<String> unassignedSqls;
    private final TreeSet<Integer> readersAwaitingSplit;
    private final Map<Integer, TdengineSplit> assignmentSqls;
    private final SplitEnumeratorContext<TdengineSplit> context;
    private final Boundedness boundedness;
    private final SourceSplitSql sourceSql;
    private final int readerCount;
    private int taskCount = 1;

    private boolean isInitFinished = false;

    public TdengineSourceEnumerator(SplitEnumeratorContext<TdengineSplit> context,
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
            splitByTimestamp();
        } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TAG) {
            splitByTags();
        } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TABLE) {
            splitByTables();
        } else {
            if (Strings.isNullOrEmpty(this.sourceSql.getSql())) {
                String sql = "select " + this.sourceSql.getSelect()
                        + " from `" + this.sourceSql.getTableName() + "` ";
                if (!this.sourceSql.getWhere().isEmpty()) {
                    sql += "where " + this.sourceSql.getWhere();
                }
                this.unassignedSqls.push(sql);
            } else {
                this.unassignedSqls.push(this.sourceSql.getSql());
            }
        }

        if (this.unassignedSqls.size() > this.readerCount) {
            taskCount = this.unassignedSqls.size() / this.readerCount;
            if ((this.unassignedSqls.size() % this.readerCount) > 0) {
                taskCount++;
            }
        }
        isInitFinished = true;
    }

    private void splitByTimestamp() {
        TimestampSplitInfo timestampSplitInfo = sourceSql.getTimestampSplitInfo();
        if (timestampSplitInfo != null && !Strings.isNullOrEmpty(timestampSplitInfo.getFieldName()) && timestampSplitInfo.getEndTime().after(timestampSplitInfo.getStartTime())) {
            long timeDifference = timestampSplitInfo.getEndTime().getTime() - timestampSplitInfo.getStartTime().getTime();
            long nCount = 0;
            if (timestampSplitInfo.getInterval() > 0) {
                nCount = timeDifference / timestampSplitInfo.getInterval();
            }

            if (nCount == 0) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + timestampSplitInfo.getFieldName() + " >= " + timestampSplitInfo.getStartTime().getTime()
                        + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime().getTime();
                this.unassignedSqls.push(sql);
            } else {
                long startTime = timestampSplitInfo.getStartTime().getTime();
                boolean bRemainder = timeDifference % timestampSplitInfo.getInterval() > 0;
                for (int i = 0; i < nCount; i++) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + startTime + timestampSplitInfo.getInterval();
                    this.unassignedSqls.push(sql);
                    startTime += timestampSplitInfo.getInterval();
                }

                if (bRemainder) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime().getTime();
                    this.unassignedSqls.push(sql);
                }
            }
        }
    }

    private void splitByTags() {
        List<String> tags = sourceSql.getTagList();
        if (tags != null && !tags.isEmpty()) {
            for (int i = 0; i < tags.size(); i++) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + tags.get(i);
                this.unassignedSqls.push(sql);
            }
        }
    }

    private void splitByTables() {
        List<String> tableList = sourceSql.getTableList();
        if (tableList != null && !tableList.isEmpty()) {
            for (int i = 0; i < tableList.size(); i++) {
                String sql = "select " + this.sourceSql.getSelect()
                        + " from `" + tableList.get(i) + "` ";
                if (!this.sourceSql.getWhere().isEmpty()) {
                    sql += "where " + this.sourceSql.getWhere();
                }
                this.unassignedSqls.push(sql);
            }
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
            TdengineSplit tdengineSplit = new TdengineSplit("" + subtaskId);
            for (int i = 0; i < this.taskCount; i++) {
                String taskSplit = unassignedSqls.pop();
                tdengineSplit.addTaskSplit(taskSplit);
            }
            assignmentSqls.put(subtaskId, tdengineSplit);
            context.assignSplit(tdengineSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public TdengineSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new TdengineSourceEnumState(this.unassignedSqls, this.assignmentSqls, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {

    }
}
