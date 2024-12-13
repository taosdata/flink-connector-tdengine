package com.taosdata.flink.source.enumerator;

import com.taosdata.flink.source.split.TDengineSplit;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TdengineSourceEnumState {
    private boolean isInitFinished;
    private final Deque<TDengineSplit> unassignedSqls;
    private final TreeSet<TDengineSplit> assignmentSqls;
    public TdengineSourceEnumState(Deque<TDengineSplit> unassignedSqls, TreeSet<TDengineSplit> assignmentSqls, boolean isInitFinished) {
        this.isInitFinished = isInitFinished;
        this.unassignedSqls = unassignedSqls;
        this.assignmentSqls = assignmentSqls;
    }

    public Deque<TDengineSplit> getUnassignedSqls() {
        return unassignedSqls;
    }

    public TreeSet<TDengineSplit> getAssignmentSqls() {
        return assignmentSqls;
    }

    public boolean isInitFinished() {
        return isInitFinished;
    }
}
