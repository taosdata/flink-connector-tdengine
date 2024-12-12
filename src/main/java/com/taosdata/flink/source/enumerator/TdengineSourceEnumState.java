package com.taosdata.flink.source.enumerator;

import com.taosdata.flink.source.split.TDengineSplit;

import java.util.Deque;
import java.util.List;
import java.util.Map;

public class TdengineSourceEnumState {
    private boolean isInitFinished;
    private final Deque<String> unassignedSqls;
    private final List<String> assignmentSqls;
    public TdengineSourceEnumState(Deque<String> unassignedSqls, List<String> assignmentSqls, boolean isInitFinished) {
        this.isInitFinished = isInitFinished;
        this.unassignedSqls = unassignedSqls;
        this.assignmentSqls = assignmentSqls;
    }

    public Deque<String> getUnassignedSqls() {
        return unassignedSqls;
    }

    public List<String> getAssignmentSqls() {
        return assignmentSqls;
    }

    public boolean isInitFinished() {
        return isInitFinished;
    }
}
