package com.taosdata.flink.sink.entity;

import java.util.List;

public class SuperTableData extends TaosSinkData {
    private String superTableName;
    private List<String> columnNames;
    private List<String> tagNames;
    private List<SubTableData> subTableDataList;

    public SuperTableData(String dbName) {
        super(dbName);
    }

    public String getSuperTableName() {
        return superTableName;
    }

    public void setSuperTableName(String superTableName) {
        this.superTableName = superTableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<String> getTagNames() {
        return tagNames;
    }

    public void setTagNames(List<String> tagNames) {
        this.tagNames = tagNames;
    }


    public List<SubTableData> getSubTableDataList() {
        return subTableDataList;
    }

    public void setSubTableDataList(List<SubTableData> subTableDataList) {
        this.subTableDataList = subTableDataList;
    }
}
