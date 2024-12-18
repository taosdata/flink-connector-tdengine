package com.taosdata.flink.source.entity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SourceRecords<V> implements Iterable<V>{
    private List<V> records;


    public SourceRecords() {
        this.records = new ArrayList<>();
    }

    public void addSourceRecord(V record) {
        this.records.add(record);
    }

    public List<V> getRecords() {
        return records;
    }

    @Override
    public Iterator<V> iterator() {
        return this.records.iterator();
    }

    public boolean isEmpty() {
        return this.records.isEmpty();
    }
}
