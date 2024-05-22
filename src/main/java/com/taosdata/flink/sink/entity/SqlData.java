package com.taosdata.flink.sink.entity;


import java.util.List;

public class SqlData extends TaosSinkData{
    private List<String> sqlList;
    public SqlData(String dbName, List<String> sqlList) {
        super(dbName);
        this.sqlList = sqlList;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }
}
