package com.taosdata.flink.source.enumerator;

import com.taosdata.flink.source.split.TdengineSplit;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TdengineSourceEnumState {
    private boolean isInitFinished;
    private final Deque<String> unassignedSqls;
    private final Map<Integer, TdengineSplit> assignmentSqls;
    public TdengineSourceEnumState(Deque<String> unassignedSqls, Map<Integer, TdengineSplit> assignmentSqls, boolean isInitFinished) {
        this.isInitFinished = isInitFinished;
        this.unassignedSqls = unassignedSqls;
        this.assignmentSqls = assignmentSqls;
    }



    public Deque<String> getReadersAwaitingSplit() {
        return unassignedSqls;
    }

    public Map<Integer, TdengineSplit> getAssignmentSqls() {
        return assignmentSqls;
    }
}
