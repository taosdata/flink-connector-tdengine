package com.taosdata.flink.common;

import com.taosdata.flink.sink.entity.SinkError;
import com.taosdata.flink.sink.entity.SinkErrorNumbers;

import java.sql.SQLException;

public class Utils {

    public static String trimBackticks(String input) throws SQLException {
        if (input == null || input.isEmpty()) {
            return input;
        }

        if (input.equals("`")) {
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_SINK_FIELD_NAME, "Field name reverse quotation mark mismatch, fieldName=" + input);
        }

        int start = 0;
        int end = input.length();

        // Check if there are reverse quotes at the beginning
        if (input.startsWith("`")) {
            start = 1;
        } else {
            return input;
        }

        // Check if there are reverse quotes at the end
        if (input.endsWith("`")) {
            end--;
        }

        // If the string only has reverse quotes
        if (end == 0 || start > end) {
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_SINK_FIELD_NAME, "Field name reverse quotation mark mismatch, fieldName=" + input);
        }

        return input.substring(start, end);
    }

    public static boolean isJDBCError(int errorCode) {
        return errorCode >= 0x2300 && errorCode <= 0x23ff;
    }
}
