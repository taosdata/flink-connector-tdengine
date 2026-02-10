package com.taosdata.flink.source.entity;

import com.taosdata.flink.source.split.TDengineSplit;

import java.sql.ResultSetMetaData;
import java.util.Iterator;

/** Data structure to describe a set of T. */
public final class SplitResultRecords<V> {

    private ResultSetMetaData metaData;
    private SourceRecords<V> sourceRecords;
    private TDengineSplit tdengineSplit;

    public SplitResultRecords() {
    }

    public void setSourceRecords(SourceRecords sourceRecords) {
         this.sourceRecords = sourceRecords;
    }
    public SourceRecords getSourceRecords() {
        return sourceRecords;
    }

    public Iterator<V> iterator() {
        return sourceRecords.iterator();
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }
    public void setMetaData(ResultSetMetaData metaData) {this.metaData = metaData;}


    public TDengineSplit getTdengineSplit() {
        return tdengineSplit;
    }

    public void setTDengineSplit(TDengineSplit tdengineSplit) {
        this.tdengineSplit = new TDengineSplit(tdengineSplit);
    }
}
