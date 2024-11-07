package com.taosdata.flink.source.entity;

import java.sql.Timestamp;

public class TimestampSplitInfo {
    private Timestamp startTime;
    private Timestamp endTime;
    private String fieldName;
    private long interval;

    public TimestampSplitInfo(Timestamp startTime, Timestamp endTime, String fieldName, long interval) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.fieldName = fieldName;
        this.interval = interval;
    }


    public Timestamp getStartTime() {
        return startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public long getInterval() {
        return interval;
    }

    public String getFieldName() {
        return fieldName;
    }
}
