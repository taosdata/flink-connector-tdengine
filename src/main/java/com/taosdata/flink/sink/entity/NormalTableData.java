package com.taosdata.flink.sink.entity;

import java.util.List;

public class NormalTableData extends TaosSinkData {
    private String tableName;
    private List<String> columNames;
    private List<ColumParam> columParams;

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

    public List<String> getColumNames() {
        return columNames;
    }

    public void setColumNames(List<String> columNames) {
        this.columNames = columNames;
    }

    public List<ColumParam> getColumParams() {
        return columParams;
    }

    public void setColumParams(List<ColumParam> columParams) {
        this.columParams = columParams;
    }
}
