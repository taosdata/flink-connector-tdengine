package com.taosdata.flink.sink.entity;

public class TDengineType {
    public static final int TSDB_DATA_TYPE_BOOL = 1;
    public static final int TSDB_DATA_TYPE_TINYINT = 2;
    public static final int TSDB_DATA_TYPE_SMALLINT = 3;
    public static final int TSDB_DATA_TYPE_INT = 4;
    public static final int TSDB_DATA_TYPE_BIGINT = 5;
    public static final int TSDB_DATA_TYPE_FLOAT = 6;
    public static final int TSDB_DATA_TYPE_DOUBLE = 7;
    public static final int TSDB_DATA_TYPE_VARCHAR = 8;
    public static final int TSDB_DATA_TYPE_BINARY = TSDB_DATA_TYPE_VARCHAR;
    public static final int TSDB_DATA_TYPE_TIMESTAMP = 9;
    public static final int TSDB_DATA_TYPE_NCHAR = 10;
    public static final int TSDB_DATA_TYPE_JSON = 15;           //json
    public static final int TSDB_DATA_TYPE_VARBINARY = 16;     //varbinary
    public static final int TSDB_DATA_TYPE_GEOMETRY = 20;     //geometry
}
