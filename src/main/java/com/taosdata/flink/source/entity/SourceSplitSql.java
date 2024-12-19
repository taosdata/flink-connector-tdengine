package com.taosdata.flink.source.entity;

import com.sun.org.apache.bcel.internal.generic.PUSH;

import java.io.Serializable;
import java.util.List;

public class SourceSplitSql implements Serializable {
    private String select;
    private String tableName;

    private String where;

    private String other;

    private SplitType splitType;
    private String sql;

    private List<String> tableList;

    private List<String> tagList;

    private TimestampSplitInfo timestampSplitInfo;

    public SourceSplitSql(String select, String tableName, String where, String other, SplitType splitType) {
        this.select = select;
        this.tableName = tableName;
        this.where = where;
        this.other = other;
        this.splitType = splitType;
        this.sql = "";
    }
    public SourceSplitSql() {
        this.select = "";
        this.tableName = "";
        this.where = "";
        this.splitType = null;
        this.sql = "";
    }
    public SourceSplitSql(String sql) {
        this.select = "";
        this.tableName = "";
        this.where = "";
        this.splitType = SplitType.SPLIT_TYPE_SQL;
        this.sql = sql;
    }
    public String getSelect() {
        return select;
    }

    public SourceSplitSql setSelect(String select) {
        this.select = select;
        return this;
    }
    public String getTableName() {
        return tableName;
    }
    public SourceSplitSql setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
    public String getWhere() {
        return where;
    }
    public SourceSplitSql setWhere(String where) {
        this.where = where;
        return this;
    }
    public SplitType getSplitType() {
        return splitType;
    }
    public SourceSplitSql setSplitType(SplitType splitType) {
        this.splitType = splitType;
        return this;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public SourceSplitSql setTableList(List<String> tableList) {
        this.tableList = tableList;
        return this;
    }

    public List<String> getTagList() {
        return tagList;
    }

    public SourceSplitSql setTagList(List<String> tagList) {
        this.tagList = tagList;
        return this;
    }

    public TimestampSplitInfo getTimestampSplitInfo() {
        return timestampSplitInfo;
    }

    public SourceSplitSql setTimestampSplitInfo(TimestampSplitInfo timestampSplitInfo) {
        this.timestampSplitInfo = timestampSplitInfo;
        return this;
    }

    public String getSql() {
        return sql;
    }
    public SourceSplitSql setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public String getOther() {
        return other;
    }

    public SourceSplitSql setOther(String other) {
        this.other = other;
        return this;
    }
}
