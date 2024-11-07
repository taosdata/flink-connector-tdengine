package com.taosdata.flink.source.entity;

import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SourceError {
    private static final Map<Integer, String> SourceErrorMap = new HashMap<>();

    static {
        SourceErrorMap.put(SourceErrorNumbers.ERROR_SERVER_ADDRESS, "Service address configuration error");
        SourceErrorMap.put(SourceErrorNumbers.ERROR_TMQ_GROUP_ID_CONFIGURATION, "Consumer group id configuration error");
        SourceErrorMap.put(SourceErrorNumbers.ERROR_TMQ_TOPIC, "Tmq topic configuration error");
        SourceErrorMap.put(SourceErrorNumbers.ERROR_CONVERT_NOT_PROVIDED, "Data conversion function not provided");
    }

    public static SQLException createSQLException(int errorCode) {
        String message;
        if (SourceErrorNumbers.contains(errorCode))
            message = SourceErrorMap.get(errorCode);
        else
            message = SourceErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);
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
        String message = SourceErrorMap.get(errorCode);
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
            message = SourceErrorMap.get(errorCode);
        else
            message = SourceErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalArgumentException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SourceErrorMap.get(errorCode);
        else
            message = SourceErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode, String message) {
        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static IllegalStateException createIllegalStateException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = SourceErrorMap.get(errorCode);
        else
            message = SourceErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalStateException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static TimeoutException createTimeoutException(int errorCode, String message) {
        return new TimeoutException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }
}