package com.taosdata.flink.sink.entity;

import java.util.List;

public class SupperTableData extends TaosSinkData {
    private String supperTableName;
    private List<String> columNames;
    private List<String> tagNames;
    private List<SubTableData> subTableDataList;

    public SupperTableData(String dbName) {
        super(dbName);
    }

    public String getSupperTableName() {
        return supperTableName;
    }

    public void setSupperTableName(String supperTableName) {
        this.supperTableName = supperTableName;
    }

    public List<String> getColumNames() {
        return columNames;
    }

    public void setColumNames(List<String> columNames) {
        this.columNames = columNames;
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
