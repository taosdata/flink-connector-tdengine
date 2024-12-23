package com.taosdata.flink.common;

public class VersionComparator {
    public static int compareVersion(String version1, String version2) {
        // 检查输入是否为 null 或空字符串
        if (version1 == null || version2 == null) {
            throw new IllegalArgumentException("Version strings cannot be null");
        }

        String[] v1Parts = version1.split("\\.");
        String[] v2Parts = version2.split("\\.");

        try {
            for (int i = 0; i < v2Parts.length; i++) {
                // 获取当前部分的值，如果没有则为 0
                int v1 = i < v1Parts.length ? Integer.parseInt(v1Parts[i]) : 0;
                int v2 = i < v2Parts.length ? Integer.parseInt(v2Parts[i]) : 0;

                // 比较当前部分
                if (v1 < v2) {
                    return -1; // version1 小于 version2
                } else if (v1 > v2) {
                    return 1; // version1 大于 version2
                }
            }
            return 0; // 版本相等
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version part version1: " + version1 + " version2" + version2);
        }

    }
}
