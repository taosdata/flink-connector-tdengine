package com.taosdata.flink.sink.entity;

import java.io.Serializable;

public class SinkMetaInfo implements Serializable {
    private  boolean isTag;
    private  DataType fieldType;
    private  String fieldName;
    private  int length;
    public SinkMetaInfo() {

    }

    public SinkMetaInfo(boolean isTag, DataType fieldType, String fieldName, int length) {
        this.isTag = isTag;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.length = length;
    }

    public boolean isTag() {
        return isTag;
    }

    public DataType getFieldType() {
        return fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getLength() {
        return length;
    }

    public void setTag(boolean tag) {
        isTag = tag;
    }

    public void setFieldType(DataType fieldType) {
        this.fieldType = fieldType;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
