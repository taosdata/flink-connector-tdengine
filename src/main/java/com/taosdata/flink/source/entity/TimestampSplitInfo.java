package com.taosdata.flink.source.entity;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
    public TimestampSplitInfo(long startTime, long endTime, String fieldName, long interval) {
        this.startTime = new Timestamp(startTime);
        this.endTime = new Timestamp(endTime);
        this.fieldName = fieldName;
        this.interval = interval;
    }

    public TimestampSplitInfo(String startTime, String endTime, String fieldName, long interval, SimpleDateFormat dateFormat) throws ParseException {
        java.util.Date date = dateFormat.parse(startTime);
        this.startTime = new Timestamp(date.getTime());
        date = dateFormat.parse(endTime);
        this.endTime = new Timestamp(date.getTime());
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
