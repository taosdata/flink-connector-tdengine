package com.taosdata.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.*;

public class TDengineSplit implements SourceSplit, Comparable<TDengineSplit> {
    protected String splitId;
    private List<String> taskList;

    private Iterator<String> currTaskIterator;

    private List<String> unexecutedTasks;

    private List<String> finishList;

    public TDengineSplit(String splitId) {
        this.splitId = splitId;
        this.finishList = new ArrayList<>();
        this.taskList = new ArrayList<>();
    }

    public TDengineSplit(String splitId, List<String> taskList, List<String> finishList) {
        this.splitId = splitId;

        if (taskList == null) {
            this.taskList = new ArrayList<>();
        } else {
            this.taskList = taskList;
        }

        if (finishList == null) {
            this.finishList = new ArrayList<>();
        } else {
            this.finishList = finishList;
        }
    }

    public TDengineSplit(TDengineSplit split) {
        this.splitId = split.splitId();
        this.finishList = new ArrayList<>(split.finishList);
        this.taskList = new ArrayList<>(split.taskList);
    }

    public void updateSplit(TDengineSplit split) {
        this.splitId = split.splitId();
        this.finishList = new ArrayList<>(split.finishList);
        this.taskList = new ArrayList<>(split.taskList);
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    public void setTaskSplits(List<String> taskList) {
        this.taskList.addAll(taskList);
//        currTaskIterator = this.taskList.iterator();
    }

    public List<String> getTaskSplits() {
        return taskList;
    }

    public String getNextTaskSplit() {
        if (taskList.isEmpty()) {
            return "";
        }

        if (currTaskIterator == null || unexecutedTasks == null || unexecutedTasks.isEmpty()) {
            if (finishList == null || finishList.isEmpty()) {
                unexecutedTasks = taskList;
                currTaskIterator = unexecutedTasks.iterator();
            } else {
                if (finishList.size() < taskList.size()) {
                    unexecutedTasks = taskList.subList(finishList.size(), taskList.size());
                    currTaskIterator = unexecutedTasks.iterator();
                }
            }
        }

        if (currTaskIterator != null && currTaskIterator.hasNext()) {
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

    @Override
    public int compareTo(TDengineSplit o) {
        return o.splitId.compareTo(this.splitId);
    }
}
