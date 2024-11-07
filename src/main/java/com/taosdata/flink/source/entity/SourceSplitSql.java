package com.taosdata.flink.source.entity;

import com.sun.org.apache.bcel.internal.generic.PUSH;

import java.io.Serializable;
import java.util.List;

public class SourceSplitSql implements Serializable {
    private String select;
    private String tableName;

    private String where;

    private SplitType splitType;

    private List<String> tableList;

    private List<String> tagList;

    private TimestampSplitInfo timestampSplitInfo;

    public SourceSplitSql(String select, String tableName, String where, SplitType splitType) {
        this.select = select;
        this.tableName = tableName;
        this.where = where;
        this.splitType = splitType;
    }

    public String getSelect() {
        return select;
    }

    public String getTableName() {
        return tableName;
    }

    public String getWhere() {
        return where;
    }

    public SplitType getSplitType() {
        return splitType;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public void setTableList(List<String> tableList) {
        this.tableList = tableList;
    }

    public List<String> getTagList() {
        return tagList;
    }

    public void setTagList(List<String> tagList) {
        this.tagList = tagList;
    }

    public TimestampSplitInfo getTimestampSplitInfo() {
        return timestampSplitInfo;
    }

    public void setTimestampSplitInfo(TimestampSplitInfo timestampSplitInfo) {
        this.timestampSplitInfo = timestampSplitInfo;
    }
}
