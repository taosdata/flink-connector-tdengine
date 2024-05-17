package com.taosdata.flink.sink;

import java.util.List;

public class StatementData {
    private int mode = 1; //1列模式，2行模式
    private String dbName;
    private String supperTableName;
    private String tableName;
    private List<String> fieldNames;
    private List<String> tagFieldNames;
    private List<Param> tagParams;
    private List<StatementParam> columParams;

    private List<List<Param>> lineParams;

    public StatementData() {}

    public List<StatementParam> getColumParams() {
        return columParams;
    }

    public void setColumParams(List<StatementParam> columParams) {
        this.columParams = columParams;
    }

    public String getSupperTableName() {
        return supperTableName;
    }

    public void setSupperTableName(String supperTableName) {
        this.supperTableName = supperTableName;
    }

    public List<Param> getTagParams() {
        return tagParams;
    }

    public void setTagParams(List<Param> tagParams) {
        this.tagParams = tagParams;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public List<String> getTagFieldNames() {
        return tagFieldNames;
    }

    public void setTagFieldNames(List<String> tagFieldNames) {
        this.tagFieldNames = tagFieldNames;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public List<List<Param>> getLineParams() {
        return lineParams;
    }

    public void setLineParams(List<List<Param>> lineParams) {
        this.lineParams = lineParams;
    }
}
