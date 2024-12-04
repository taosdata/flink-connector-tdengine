package com.taosdata.flink.cdc.entity;

import java.util.Objects;

public class CdcTopicPartition {
    private static final long serialVersionUID = -613627415771699627L;
    private int hash = 0;
    private final long position;
    private final String topic;
    private final int vGroupId;

    public CdcTopicPartition(String topic, long position, int vGroupId) {
        this.position = position;
        this.topic = topic;
        this.vGroupId = vGroupId;
    }

    public long getPartition() {
        return this.position;
    }

    public String getTopic() {
        return this.topic;
    }

    public int getvGroupId() {return vGroupId;}
    public int hashCode() {
        if (this.hash != 0) {
            return this.hash;
        } else {
            int result = 31 + this.vGroupId;
            result = 31 * result + Objects.hashCode(this.topic);
            this.hash = result;
            return result;
        }
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            CdcTopicPartition other = (CdcTopicPartition)obj;
            return this.vGroupId == other.vGroupId && Objects.equals(this.topic, other.topic);
        }
    }

    public String toString() {
        return this.topic + "-" + this.vGroupId;
    }
}
