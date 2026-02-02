package com.taosdata.flink.source.entity;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Data structure to describe a set of T. */
public final class SplitResultRecord {

    private final List<Object> sourceRecord;

    private ResultSetMetaData metaData;

    public SplitResultRecord(ResultSetMetaData metaData) {
        this.metaData = metaData;
        this.sourceRecord = new ArrayList<>();
    }
    public void addObject(Object o) {
        sourceRecord.add(o);
    }
    public List<Object> getSourceRecordList() {
        return sourceRecord;
    }

    public Iterator<Object> iterator() {
        return sourceRecord.iterator();
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }
}
