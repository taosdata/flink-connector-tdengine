package com.taosdata.flink;

import com.taosdata.jdbc.tmq.ReferenceDeserializer;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class ResultBean {
    private Timestamp ts;
    private int voltage;
    private Float current;
    private Float phase;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public Float getCurrent() {
        return current;
    }

    public void setCurrent(Float current) {
        this.current = current;
    }


    public Float getPhase() {
        return phase;
    }

    public void setPhase(Float phase) {
        this.phase = phase;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ResultBean{");
        sb.append("ts=").append(ts);
        sb.append(", current=").append(current);
        sb.append(", voltage=").append(voltage);
        sb.append(", phase='").append(phase);
        sb.append('}');
        return sb.toString();
    }
}