package com.taosdata.flink.sink.entity;

import java.io.Serializable;
import java.util.List;

public class TDengineSinkRecord implements Serializable {

    private final List<Object> columnParams;

    public TDengineSinkRecord(List<Object> columnParams) {
        this.columnParams = columnParams;
    }
    
    public List<Object> getColumnParams() {
        return columnParams;
    }

}
