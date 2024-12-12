package com.taosdata.flink.source.entity;

import java.util.HashSet;
import java.util.Set;

public class SourceErrorNumbers {

    public static final int ERROR_SERVER_ADDRESS = 0x2000;
    public static final int ERROR_TMQ_GROUP_ID_CONFIGURATION = 0x2001;

    public static final int ERROR_TMQ_TOPIC = 0x2002;

    public static final int ERROR_CONVERT_NOT_PROVIDED = 0x2003;


    private static final Set<Integer> errorNumbers = new HashSet<>();

    static {
        errorNumbers.add(ERROR_SERVER_ADDRESS);
        errorNumbers.add(ERROR_TMQ_GROUP_ID_CONFIGURATION);
        errorNumbers.add(ERROR_TMQ_TOPIC);
        errorNumbers.add(ERROR_CONVERT_NOT_PROVIDED);
    }

    private SourceErrorNumbers() {
    }

    public static boolean contains(int errorNumber) {
        return errorNumbers.contains(errorNumber);
    }
}