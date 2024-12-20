package com.taosdata.flink.source.enumerator;

import com.google.common.base.Strings;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TDengineSourceEnumerator implements SplitEnumerator<TDengineSplit, TDengineSourceEnumState> {
    private Deque<TDengineSplit> unassignedSplits;
    private TreeSet<TDengineSplit> assignmentSplits;
    private final SplitEnumeratorContext<TDengineSplit> context;
    private final Boundedness boundedness;
    private final SourceSplitSql sourceSql;
    private final int readerCount;
    private int taskCount = 1;

    private boolean isInitFinished = false;


    public TDengineSourceEnumerator(SplitEnumeratorContext<TDengineSplit> context,
                                    Boundedness boundedness, SourceSplitSql sourceSql) {
        this.assignmentSplits = new TreeSet<>();
        this.unassignedSplits = new ArrayDeque<>();
        this.context = context;
        this.boundedness = boundedness;
        this.sourceSql = sourceSql;
        this.readerCount = context.currentParallelism();
    }

    public TDengineSourceEnumerator(SplitEnumeratorContext<TDengineSplit> context,
                                    Boundedness boundedness, SourceSplitSql sourceSql, TDengineSourceEnumState splitsState) {
        this.assignmentSplits = new TreeSet<>();
        this.unassignedSplits = new ArrayDeque<>();
        this.context = context;
        this.boundedness = boundedness;
        this.sourceSql = sourceSql;
        this.readerCount = context.currentParallelism();
        if (splitsState != null && splitsState.isInitFinished()) {
            assignmentSplits = splitsState.getAssignmentSqls();
            unassignedSplits = splitsState.getUnassignedSqls();
            this.isInitFinished = true;
        }
    }

    @Override
    public void start() {
        if (!this.isInitFinished) {
            this.unassignedSplits.clear();
            this.assignmentSplits.clear();
            Deque<String> unassignedSql = new ArrayDeque<>();
            if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TIMESTAMP) {
                unassignedSql = splitByTimestamp();
            } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TAG) {
                unassignedSql = splitByTags();
            } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TABLE) {
                unassignedSql = splitByTables();
            } else {
                if (Strings.isNullOrEmpty(this.sourceSql.getSql())) {
                    String sql = "select " + this.sourceSql.getSelect()
                            + " from `" + this.sourceSql.getTableName() + "` ";
                    if (!this.sourceSql.getWhere().isEmpty()) {
                        sql += "where " + this.sourceSql.getWhere();
                    }
                    unassignedSql.push(sql);
                } else {
                    unassignedSql.push(this.sourceSql.getSql());
                }
            }

            if (unassignedSql.size() > this.readerCount) {
                taskCount = unassignedSql.size() / this.readerCount;
                if ((unassignedSql.size() % this.readerCount) > 0) {
                    taskCount++;
                }
            }

            for (int i = 0; i < this.readerCount && !unassignedSql.isEmpty(); i++) {
                TDengineSplit tdengineSplit = new TDengineSplit("reader_no_" + i);
                List<String> taskSplits = new ArrayList<>(taskCount);
                for (int j = 0; j < this.taskCount; j++) {
                    String taskSplit = unassignedSql.pop();
                    if (Strings.isNullOrEmpty(taskSplit)) {
                        break;
                    }
                    taskSplits.add(taskSplit);
                }
                if (taskSplits.isEmpty()) {
                    break;
                }
                tdengineSplit.setTaskSplits(taskSplits);
                this.unassignedSplits.push(tdengineSplit);
            }
        }

        isInitFinished = true;
    }

    private Deque<String> splitByTimestamp() {
        Deque<String> unassignedSql = new ArrayDeque<>();
        TimestampSplitInfo timestampSplitInfo = sourceSql.getTimestampSplitInfo();
        if (timestampSplitInfo != null && !Strings.isNullOrEmpty(timestampSplitInfo.getFieldName()) && timestampSplitInfo.getEndTime() > timestampSplitInfo.getStartTime()) {
            long timeDifference = timestampSplitInfo.getEndTime() - timestampSplitInfo.getStartTime();
            long nCount = 0;
            if (timestampSplitInfo.getInterval() > 0) {
                nCount = timeDifference / timestampSplitInfo.getInterval();
            }

            if (nCount == 0) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + timestampSplitInfo.getFieldName() + " >= " + timestampSplitInfo.getStartTime()
                        + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime();
                unassignedSql.push(sql);
            } else {
                long startTime = timestampSplitInfo.getStartTime();
                boolean bRemainder = timeDifference % timestampSplitInfo.getInterval() > 0;
                for (int i = 0; i < nCount; i++) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + (startTime + timestampSplitInfo.getInterval());
                    unassignedSql.push(sql);
                    startTime += timestampSplitInfo.getInterval();
                }

                if (bRemainder) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime();
                    unassignedSql.push(sql);
                }
            }
        }
        return unassignedSql;
    }

    private Deque<String> splitByTags() {
        Deque<String> unassignedSqls = new ArrayDeque<>();
        List<String> tags = sourceSql.getTagList();
        if (tags != null && !tags.isEmpty()) {
            for (int i = 0; i < tags.size(); i++) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + tags.get(i);
                unassignedSqls.push(sql);
            }
        }
        return unassignedSqls;
    }

    private Deque<String> splitByTables() {
        Deque<String> unassignedSqls = new ArrayDeque<>();
        List<String> tableList = sourceSql.getTableList();
        if (tableList != null && !tableList.isEmpty()) {
            for (int i = 0; i < tableList.size(); i++) {
                String sql = "select " + this.sourceSql.getSelect()
                        + " from `" + tableList.get(i) + "` ";
                if (!this.sourceSql.getWhere().isEmpty()) {
                    sql += "where " + this.sourceSql.getWhere();
                }
                if (!this.sourceSql.getOther().isEmpty()) {
                    sql += " " + sourceSql.getOther();
                }
                unassignedSqls.push(sql);
            }
        }
        return unassignedSqls;
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
    public void addSplitsBack(List<TDengineSplit> splits, int subtaskId) {
        TreeSet<TDengineSplit> splitSet = new TreeSet<>(unassignedSplits);
        splitSet.addAll(splits);
        unassignedSplits.addAll(splitSet);
        if (context.registeredReaders().containsKey(subtaskId)) {
            addReader(subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        checkReaderRegistered(subtaskId);
        if (!unassignedSplits.isEmpty()) {
            TDengineSplit tdengineSplit = unassignedSplits.pop();
            assignmentSplits.add(tdengineSplit);
            context.assignSplit(tdengineSplit, subtaskId);
        } else {
            context.assignSplit(new TDengineSplit("empty_" + subtaskId), subtaskId);
        }

        if (unassignedSplits.isEmpty()) {
            context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
        }
    }

    @Override
    public TDengineSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new TDengineSourceEnumState(this.unassignedSplits, this.assignmentSplits, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {

    }
}
