package com.taosdata.flink.cdc.enumerator;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;

import java.util.Deque;
import java.util.List;

public class TDengineCdcEnumState {
    private boolean isInitFinished;
    private Deque<TDengineCdcSplit> unassignedCdcSplits;
    private List<TDengineCdcSplit> assignmentCdcSplits;
    public TDengineCdcEnumState(Deque<TDengineCdcSplit> unassignedCdcSplits, List<TDengineCdcSplit> assignmentCdcSplits, boolean isInitFinished) {
        this.isInitFinished = isInitFinished;
        this.unassignedCdcSplits = unassignedCdcSplits;
        this.assignmentCdcSplits = assignmentCdcSplits;
    }



    public Deque<TDengineCdcSplit> getUnassignedCdcSplits() {
        return unassignedCdcSplits;
    }

    public List<TDengineCdcSplit> getAssignmentCdcSplits() {
        return assignmentCdcSplits;
    }

    public boolean isInitFinished() {
        return isInitFinished;
    }
}
