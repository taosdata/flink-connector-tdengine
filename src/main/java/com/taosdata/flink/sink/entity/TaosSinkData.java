package com.taosdata.flink.sink.entity;

public class TaosSinkData {
    private String dbName;

    public String getDbName() {
        return dbName;
    }
    TaosSinkData(String dbName) {
        this.dbName = dbName;
    }
}
