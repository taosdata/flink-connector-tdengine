package com.taosdata.flink.sink.entity;

import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SinkError {
    private static final Map<Integer, String> SinkErrorMap = new HashMap<>();

    static {
        SinkErrorMap.put(SinkErrorNumbers.ERROR_DB_NAME_NULL, "db name must be set");
        SinkErrorMap.put(SinkErrorNumbers.ERROR_TABLE_NAME_NULL, "table name must be set");
        SinkErrorMap.put(SinkErrorNumbers.ERROR_INVALID_VALUE_DESERIALIZER, "invalid serialization type");
        SinkErrorMap.put(SinkErrorNumbers.ERROR_INVALID_SINK_FIELD_NAME, "invalid field name");
        SinkErrorMap.put(SinkErrorNumbers.ERROR_INVALID_SINK_FIELD_NAME, "invalid field name");
        SinkErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown error");
    }

    public static SQLException createSQLException(int errorCode) {
        String message = SinkErrorMap.get(errorCode);

        if (message == null)
            message = SinkErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return createSQLException(errorCode, message);
    }

    public static SQLException createSQLException(int errorCode, String message) {
        return new SQLException("TDengine ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
    }
}