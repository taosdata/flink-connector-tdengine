package com.taosdata.flink.sink.entity;

public class TagParam<T> {
    private DataType type;
    private T value;

    public TagParam(DataType type, T value) {
        this.type = type;
        this.value = value;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

}
