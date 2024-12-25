package com.taosdata.flink.source.enumerator;

import com.google.common.base.Strings;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TDengineSourceEnumerator implements SplitEnumerator<TDengineSplit, TDengineSourceEnumState> {
    private final Logger LOG = LoggerFactory.getLogger(TDengineSourceEnumerator.class);
    private Deque<TDengineSplit> unassignedSplits;
    private TreeSet<TDengineSplit> assignmentSplits;
    private final SplitEnumeratorContext<TDengineSplit> context;
    private final Boundedness boundedness;
    private final SourceSplitSql sourceSql;
    private final int readerCount;
    private int taskCount = 1;

    private boolean isInitFinished = false;

    private HashSet<Integer> taskIdSet;

    public TDengineSourceEnumerator(SplitEnumeratorContext<TDengineSplit> context,
                                    Boundedness boundedness, SourceSplitSql sourceSql) {
        this.assignmentSplits = new TreeSet<>();
        this.unassignedSplits = new ArrayDeque<>();
        this.taskIdSet = new HashSet<>();
        this.context = context;
        this.boundedness = boundedness;
        this.sourceSql = sourceSql;
        this.readerCount = context.currentParallelism();
        LOG.info("create TDengineSourceEnumerator object, readerCount:{}", readerCount);
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
        LOG.warn("restore TDengineCdcEnumerator object, readerCount:{}, isInitFinished:{}", readerCount, this.isInitFinished);
    }

    /**
     * Split tasks according to rules
     */
    @Override
    public void start() {
        if (!this.isInitFinished) {
            this.unassignedSplits.clear();
            this.assignmentSplits.clear();
            Deque<String> unassignedSql = new ArrayDeque<>();
            if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TIMESTAMP) {
                // divide tasks by time
                unassignedSql = splitByTimestamp();
            } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TAG) {
                // divide tasks by tag
                unassignedSql = splitByTags();
            } else if (sourceSql.getSplitType() == SplitType.SPLIT_TYPE_TABLE) {
                // divide tasks by table
                unassignedSql = splitByTables();
            } else {
                // single sql task
                if (Strings.isNullOrEmpty(this.sourceSql.getSql())) {
                    String sql = "select " + this.sourceSql.getSelect()
                            + " from `" + this.sourceSql.getTableName() + "` ";
                    if (!this.sourceSql.getWhere().isEmpty()) {
                        sql += "where " + this.sourceSql.getWhere();
                    }
                    LOG.debug("single sql task, sql：{}", sql);
                    unassignedSql.push(sql);
                } else {
                    unassignedSql.push(this.sourceSql.getSql());
                    LOG.debug("single sql task, sql：{}", this.sourceSql.getSql());
                }
            }
            // Calculate the number of tasks to be assigned to each reader based on the number of readers
            if (unassignedSql.size() > this.readerCount) {
                taskCount = unassignedSql.size() / this.readerCount;
                if ((unassignedSql.size() % this.readerCount) > 0) {
                    taskCount++;
                }
            }

            LOG.debug("Number of tasks per slice, count：{}", taskCount);

            // Create splits for each reader and assign tasks accordingly
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

    /**
     * divide tasks by time
     * @return Return SQL List, time intervals are often left-closed and right-open. For example:
     *        select * from (select  ts, `current`, voltage, phase, tbname from meters where voltage > 100)
     *        where ts >= "2024-11-12 14:55:00" and ts < "2024-11-12 15:00:00";
     */
    private Deque<String> splitByTimestamp() {
        Deque<String> unassignedSql = new ArrayDeque<>();
        TimestampSplitInfo timestampSplitInfo = sourceSql.getTimestampSplitInfo();
        if (timestampSplitInfo != null && !Strings.isNullOrEmpty(timestampSplitInfo.getFieldName()) && timestampSplitInfo.getEndTime() > timestampSplitInfo.getStartTime()) {
            long timeDifference = timestampSplitInfo.getEndTime() - timestampSplitInfo.getStartTime();
            long nCount = 0;
            // Obtain the number of separable tasks
            if (timestampSplitInfo.getInterval() > 0) {
                nCount = timeDifference / timestampSplitInfo.getInterval();
            }

            // single sql task
            if (nCount == 0) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + timestampSplitInfo.getFieldName() + " >= " + timestampSplitInfo.getStartTime()
                        + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime();
                LOG.debug("divide tasks by time, sql：{}", sql);
                unassignedSql.push(sql);
            } else {
                // Splicing SQL based on time intervals
                long startTime = timestampSplitInfo.getStartTime();
                boolean bRemainder = timeDifference % timestampSplitInfo.getInterval() > 0;
                for (int i = 0; i < nCount; i++) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + (startTime + timestampSplitInfo.getInterval());

                    LOG.debug("divide tasks by time, sql：{}", sql);
                    unassignedSql.push(sql);
                    startTime += timestampSplitInfo.getInterval();
                }

                if (bRemainder) {
                    String sql = "select * from (" + sourceSql.getSql() + ") where "
                            + timestampSplitInfo.getFieldName() + " >= " + startTime
                            + " and " + timestampSplitInfo.getFieldName() + " < " + timestampSplitInfo.getEndTime();
                    LOG.debug("divide tasks by time, sql：{}", sql);
                    unassignedSql.push(sql);
                }
            }
        }
        return unassignedSql;
    }
    /**
     * divide tasks by tag
     * @return Return SQL List. For example:
     *         select * from (select  ts, current, voltage, phase, groupid, location from meters where voltage > 100)
     *         where groupid >= 100 and location = 'SuhctA';
     */
    private Deque<String> splitByTags() {
        Deque<String> unassignedSqls = new ArrayDeque<>();
        List<String> tags = sourceSql.getTagList();
        if (tags != null && !tags.isEmpty()) {
            for (int i = 0; i < tags.size(); i++) {
                String sql = "select * from (" + sourceSql.getSql() + ") where "
                        + tags.get(i);
                LOG.debug("divide tasks by tag, sql：{}", sql);
                unassignedSqls.push(sql);
            }
        }
        return unassignedSqls;
    }


    /**
     * divide tasks by table
     * @return Return SQL List. For example:
     *         select  ts, current, voltage, phase, groupid, location from meters1 where voltage > 100
     *         select  ts, current, voltage, phase, groupid, location from meters2 where voltage > 100
     */
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
                LOG.debug("divide tasks by table, sql：{}", sql);
                unassignedSqls.push(sql);
            }
        }
        return unassignedSqls;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the TDengine source pushes splits eagerly, rather than act upon split requests.
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public void addSplitsBack(List<TDengineSplit> splits, int subtaskId) {
        LOG.warn("addSplitsBack subtaskId:{}", splits);
        if (!splits.isEmpty()) {
            TreeSet<TDengineSplit> splitSet = new TreeSet<>(unassignedSplits);
            splitSet.addAll(splits);
            unassignedSplits.addAll(splitSet);
        }
        taskIdSet.add(subtaskId);
        if (context.registeredReaders().containsKey(subtaskId)) {
            addReader(subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.trace("addReader subtaskId:{}", subtaskId);
        checkReaderRegistered(subtaskId);
        if (!taskIdSet.contains(subtaskId)) {
            if (!unassignedSplits.isEmpty()) {
                TDengineSplit tdengineSplit = unassignedSplits.pop();
                assignmentSplits.add(tdengineSplit);
                context.assignSplit(tdengineSplit, subtaskId);
                LOG.debug("addReader assigned subtaskId:{}, splitId:{}", subtaskId, tdengineSplit.splitId());
            } else {
                context.assignSplit(new TDengineSplit("empty_" + subtaskId), subtaskId);
                LOG.debug("No task assigned, send an empty task, causing the reader to hang，subtaskId:{}", subtaskId);
            }
            taskIdSet.add(subtaskId);
        }

        if (unassignedSplits.isEmpty()) {
            context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
            LOG.debug("Notify registered readers that there are no tasks assigned!");
        }
    }

    @Override
    public TDengineSourceEnumState snapshotState(long checkpointId) throws Exception {
        LOG.debug("split enumerator snapshotState, checkpointId:{}, unassigned:{}, assignment:{}",
                checkpointId, unassignedSplits.size(), assignmentSplits.size());
        return new TDengineSourceEnumState(this.unassignedSplits, this.assignmentSplits, this.isInitFinished);
    }

    @Override
    public void close() throws IOException {
        LOG.debug("split enumerator closed!");
    }
}
