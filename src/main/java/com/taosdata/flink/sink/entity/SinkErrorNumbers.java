package com.taosdata.flink.sink.entity;

import java.util.HashSet;
import java.util.Set;

public class SinkErrorNumbers {

    public static final int ERROR_DB_NAME_NULL = 0xa010;
    public static final int ERROR_TABLE_NAME_NULL = 0xa011;

    public static final int ERROR_SQL_EXECUTION_NO_RESULTS = 0x1012;

    public static final int ERROR_INVALID_VALUE_DESERIALIZER = 0xa013;

    public static final int ERROR_INVALID_SINK_FIELD_NAME = 0xa014;


    private static final Set<Integer> errorNumbers = new HashSet<>();

    static {
        errorNumbers.add(ERROR_DB_NAME_NULL);
        errorNumbers.add(ERROR_TABLE_NAME_NULL);
        errorNumbers.add(ERROR_SQL_EXECUTION_NO_RESULTS);
        errorNumbers.add(ERROR_INVALID_VALUE_DESERIALIZER);
        errorNumbers.add(ERROR_INVALID_SINK_FIELD_NAME);
    }

    private SinkErrorNumbers() {
    }
}
