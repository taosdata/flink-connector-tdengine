package com.taosdata.flink.common;

import com.google.common.base.Strings;

public class VersionComparator {
    public static int compareVersion(String version1, String version2) {
        if (Strings.isNullOrEmpty(version1) || Strings.isNullOrEmpty(version2)) {
            throw new IllegalArgumentException("Version strings cannot be null");
        }

        String[] v1Parts = version1.split("\\.");
        String[] v2Parts = version2.split("\\.");

        try {
            for (int i = 0; i < v2Parts.length; i++) {
                int v1 = i < v1Parts.length ? Integer.parseInt(v1Parts[i]) : 0;
                int v2 = i < v2Parts.length ? Integer.parseInt(v2Parts[i]) : 0;

                if (v1 < v2) {
                    return -1; // version1 gt version2
                } else if (v1 > v2) {
                    return 1; // version1 lt version2
                }
            }
            return 0; // equals
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version part version1: " + version1 + " version2" + version2);
        }

    }
}
