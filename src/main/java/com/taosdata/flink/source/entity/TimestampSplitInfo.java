package com.taosdata.flink.source.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;

public class TimestampSplitInfo implements Serializable {
    private Long startTime;
    private Long endTime;
    private String fieldName;
    private long interval;

    public TimestampSplitInfo(LocalDateTime startTime, LocalDateTime endTime, String fieldName, Duration interval, ZoneId zone) {
        if (zone == null) {
            zone = ZoneId.systemDefault();
        }
        this.startTime = startTime.atZone(zone).toInstant().toEpochMilli();;
        this.endTime = endTime.atZone(zone).toInstant().toEpochMilli();
        this.fieldName = fieldName;
        this.interval = interval.toMillis();
    }
    public TimestampSplitInfo(long startTime, long endTime, String fieldName, Duration interval) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.fieldName = fieldName;
        this.interval = interval.toMillis();
    }

    public TimestampSplitInfo(String startTime, String endTime, String fieldName, Duration interval, SimpleDateFormat dateFormat, ZoneId zone) throws ParseException {
        java.util.Date date = dateFormat.parse(startTime);
        Instant instant = date.toInstant();
        if (zone == null) {
            zone = ZoneId.systemDefault();
        }
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        this.startTime = localDateTime.atZone(zone).toInstant().toEpochMilli();

        date = dateFormat.parse(endTime);
        instant = date.toInstant();
        localDateTime = LocalDateTime.ofInstant(instant, zone);
        this.endTime = localDateTime.atZone(zone).toInstant().toEpochMilli();
        this.fieldName = fieldName;
        this.interval = interval.toMillis();
    }

    public TimestampSplitInfo(String startTime, String endTime, String fieldName, Duration interval, SimpleDateFormat dateFormat) throws ParseException {
        java.util.Date date = dateFormat.parse(startTime);
        this.startTime = new Timestamp(date.getTime()).getTime();
        date = dateFormat.parse(endTime);
        this.endTime = new Timestamp(date.getTime()).getTime();
        this.fieldName = fieldName;
        this.interval = interval.toMillis();
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public long getInterval() {
        return interval;
    }

    public String getFieldName() {
        return fieldName;
    }
}
