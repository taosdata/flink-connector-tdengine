package com.taosdata.flink.sink.entity;

import java.util.List;

public class SubTableData {
    private String tableName;

    private List<TagParam> tagParams;
    private List<ColumnParam> columnParams;

    public SubTableData() {

    }

    public List<ColumnParam> getColumParams() {
        return columnParams;
    }

    public void setColumnParams(List<ColumnParam> columnParams) {
        this.columnParams = columnParams;
    }


    public List<TagParam> getTagParams() {
        return tagParams;
    }

    public void setTagParams(List<TagParam> tagTagParams) {
        this.tagParams = tagTagParams;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
