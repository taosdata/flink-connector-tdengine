package com.taosdata.flink.sink.entity;

import java.util.List;
public class ColumParam<T> {
    private DataType type;
    private List<T> values;

    public ColumParam(DataType type, List<T> values) {
        this.type = type;
        this.values = values;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public List<T> getValues() {
        return values;
    }

    public void setValues(List<T> values) {
        this.values = values;
    }
}
