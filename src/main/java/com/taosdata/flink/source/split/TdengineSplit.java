package com.taosdata.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TdengineSplit implements SourceSplit {
    protected final String splitId;
    private final List<String> taskList;

    private List<String> finishList;
    public TdengineSplit(String splitId) {
        this.splitId = splitId;
        this.finishList = new ArrayList<>();
        this.taskList = new ArrayList<>();
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    public void addTaskSplit(String taskSplit) {
        taskList.add(taskSplit);
    }
    public List<String> getTaskSplit(String taskSplit) {
        return taskList;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdengineSplit that = (TdengineSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    public String getSql() {
        return sql;
    }

    public List<String> getFinishList() {
        return finishList;
    }
    public void setFinishList(List<String> finishList) {
        this.finishList = finishList;
    }
}
