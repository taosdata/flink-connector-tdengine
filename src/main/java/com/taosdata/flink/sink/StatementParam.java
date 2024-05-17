package com.taosdata.flink.sink;

import java.util.List;
public class StatementParam<T> {
    private DataType type;
    private List<T> values;

    public StatementParam(DataType type, List<T> values) {
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
