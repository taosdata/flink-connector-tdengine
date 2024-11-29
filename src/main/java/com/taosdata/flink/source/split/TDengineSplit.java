package com.taosdata.flink.source.split;

import com.google.common.base.Strings;
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
        currTaskIterator = this.taskList.iterator();

    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    public void addTaskSplit(List<String> taskList) {
        taskList.addAll(taskList);

    }
    public List<String> getTaskSplit() {
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

    public void finishTaskSplit(String task) {
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
