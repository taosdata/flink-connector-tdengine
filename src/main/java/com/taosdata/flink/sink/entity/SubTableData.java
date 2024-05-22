package com.taosdata.flink.sink.entity;

import java.util.List;

public class SubTableData {
    private String tableName;

    private List<TagParam> tagTagParams;
    private List<ColumParam> columParams;

    private List<List<TagParam>> lineParams;

    public SubTableData() {

    }

    public List<ColumParam> getColumParams() {
        return columParams;
    }

    public void setColumParams(List<ColumParam> columParams) {
        this.columParams = columParams;
    }


    public List<TagParam> getTagParams() {
        return tagTagParams;
    }

    public void setTagParams(List<TagParam> tagTagParams) {
        this.tagTagParams = tagTagParams;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public List<List<TagParam>> getLineParams() {
        return lineParams;
    }

    public void setLineParams(List<List<TagParam>> lineParams) {
        this.lineParams = lineParams;
    }
}
