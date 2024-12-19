package com.taosdata.flink.sink.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class TDengineSinkRecord implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SuperTableData.class);
    private String tableName;

    private final List<Object> tagParams;
    private final List<Object> columnParams;

    public TDengineSinkRecord(String tableName, List<Object> tagParams, List<Object> columnParams) {
        this.tableName = tableName;
        this.tagParams = tagParams;
        this.columnParams = columnParams;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Object> getColumnParams() {
        return columnParams;
    }

    public List<Object> getTagParams() {
        return tagParams;
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
