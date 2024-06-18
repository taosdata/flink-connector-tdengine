package com.taosdata.flink.sink.entity;

import java.util.List;

public class NormalTableData extends TaosSinkData {
    private String tableName;
    private List<String> columnNames;
    private List<ColumnParam> columnParams;

    public NormalTableData(String dbName, String tableName) {
        super(dbName);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<ColumnParam> getColumParams() {
        return columnParams;
    }

    public void setColumnParams(List<ColumnParam> columnParams) {
        this.columnParams = columnParams;
    }
}
