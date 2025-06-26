package com.taosdata.flink.common;

public class Utils {

    public static String trimBackticks(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        int start = 0;
        int end = input.length();

        // Check if there are reverse quotes at the beginning
        if (input.startsWith("`")) {
            start = 1;
        }

        // Check if there are reverse quotes at the end
        if (input.endsWith("`")) {
            end--;
        }

        // If the string only has reverse quotes
        if (start > end) {
            return "";
        }

        return input.substring(start, end);
    }
}
