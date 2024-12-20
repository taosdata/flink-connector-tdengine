package com.taosdata.flink.sink.entity;

import java.util.HashSet;
import java.util.Set;

public class SinkErrorNumbers {

    public static final int ERROR_DB_NAME_NULL = 0x1000;
    public static final int ERROR_TABLE_NAME_NULL = 0x1001;

    public static final int ERROR_SQL_EXECUTION_NO_RESULTS = 0x1002;

    public static final int ERROR_INVALID_VALUE_DESERIALIZER = 0x1003;

    public static final int ERROR_INVALID_SINK_Field_NAME = 0x1004;


    private static final Set<Integer> errorNumbers = new HashSet<>();

    static {
        errorNumbers.add(ERROR_DB_NAME_NULL);
        errorNumbers.add(ERROR_TABLE_NAME_NULL);
        errorNumbers.add(ERROR_SQL_EXECUTION_NO_RESULTS);
        errorNumbers.add(ERROR_INVALID_VALUE_DESERIALIZER);
        errorNumbers.add(ERROR_INVALID_SINK_Field_NAME);
    }

    private SinkErrorNumbers() {
    }

    public static boolean contains(int errorNumber) {
        return errorNumbers.contains(errorNumber);
    }
}
