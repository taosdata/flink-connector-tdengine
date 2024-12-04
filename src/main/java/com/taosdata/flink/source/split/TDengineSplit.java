package com.taosdata.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.*;

public class TDengineSplit implements SourceSplit {
    protected final String splitId;
    private final List<String> taskList;

    private Iterator<String> currTaskIterator;

    private List<String> finishList;
    public TDengineSplit(String splitId) {
        this.splitId = splitId;
        this.finishList = new ArrayList<>();
        this.taskList = new ArrayList<>();
    }

    public TDengineSplit(String splitId, List<String> taskList, List<String> finishList) {
        this.splitId = splitId;
        this.finishList = taskList;
        this.taskList = finishList;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    public void setTaskSplits(List<String> taskList) {
        this.taskList.addAll(taskList);
        currTaskIterator = this.taskList.iterator();
    }
    public List<String> gettasksplits() {
        return taskList;
    }

    public String getNextTaskSplit() {
        if (taskList.isEmpty()) {
            return "";
        }
        if (currTaskIterator.hasNext()) {
            return currTaskIterator.next();
        }
        return "";
    }

    public void addFinishTaskSplit(String task) {
        this.finishList.add(task);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TDengineSplit that = (TDengineSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    public List<String> getFinishList() {
        return finishList;
    }
    public void setFinishList(List<String> finishList) {
        this.finishList = finishList;
    }
}
