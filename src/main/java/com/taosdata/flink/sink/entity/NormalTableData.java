package com.taosdata.flink.sink.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NormalTableData extends TaosSinkData {
    private static final Logger log = LoggerFactory.getLogger(NormalTableData.class);
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

    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Request to string error", e);
            return null;
        }
    }

}
