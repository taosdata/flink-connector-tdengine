package com.taosdata.flink.sink.entity;

import java.util.List;

public class SubTableData {
    private String tableName;

    private List<TagParam> tagParams;
    private List<ColumParam> columParams;

    public SubTableData() {

    }

    public List<ColumParam> getColumParams() {
        return columParams;
    }

    public void setColumParams(List<ColumParam> columParams) {
        this.columParams = columParams;
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
