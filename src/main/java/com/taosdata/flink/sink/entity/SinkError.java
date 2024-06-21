package com.taosdata.flink.sink.entity;

import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SinkError {
    private static final Map<Integer, String> SinkErrorMap = new HashMap<>();

    static {
        SinkErrorMap.put(SinkErrorNumbers.ERROR_DB_NAME_NULL, "db name must be set");
        SinkErrorMap.put(SinkErrorNumbers.ERROR_TABLE_NAME_NULL, "table name must be set");
    }

    public static SQLException createSQLException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SinkErrorMap.get(errorCode);
        else
            message = SinkErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);
        return createSQLException(errorCode, message);
    }

    public static SQLException createSQLException(int errorCode, String message) {
        // throw SQLFeatureNotSupportedException
        if (errorCode == TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD)
            return new SQLFeatureNotSupportedException(message, "", errorCode);
        // throw SQLClientInfoException
        if (errorCode == TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED)
            return new SQLClientInfoException(message, null);

        if (errorCode > 0x2300 && errorCode < 0x2350)
            // JDBC exception's error number is less than 0x2350
            return new SQLException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
        if (errorCode > 0x2350 && errorCode < 0x2370)
            // JNI exception's error number is large than 0x2350
            return new SQLException("JNI ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);

        if (errorCode > 0x2370 && errorCode < 0x2400)
            return new SQLException("Consumer ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);

        return new SQLException("TDengine ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
    }

    public static RuntimeException createRuntimeException(int errorCode, Throwable t) {
        String message = SinkErrorMap.get(errorCode);
        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, t);
    }

    public static SQLWarning createSQLWarning(String message) {
        return new SQLWarning(message);
    }


    // paramter size is greater than 1
    public static SQLException undeterminedExecutionError() {
        return new SQLException("Please either call clearBatch() to clean up context first, or use executeBatch() instead", (String) null);
    }

    public static IllegalArgumentException createIllegalArgumentException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SinkErrorMap.get(errorCode);
        else
            message = SinkErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalArgumentException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SinkErrorMap.get(errorCode);
        else
            message = SinkErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode, String message) {
        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static IllegalStateException createIllegalStateException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SinkErrorMap.get(errorCode);
        else
            message = SinkErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalStateException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static TimeoutException createTimeoutException(int errorCode, String message) {
        return new TimeoutException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }
}