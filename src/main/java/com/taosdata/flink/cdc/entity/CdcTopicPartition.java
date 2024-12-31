package com.taosdata.flink.cdc.entity;

import java.util.Objects;

public class CdcTopicPartition {
    private static final long serialVersionUID = -613627415771699627L;
    private int hash = 0;
    private final Long position;
    private final String topic;
    private final Integer vGroupId;

    public CdcTopicPartition(String topic, long position, int vGroupId) {
        this.position = position;
        this.topic = topic;
        this.vGroupId = vGroupId;
        this.hash = hashCode();
    }

    public Long getPartition() {
        return this.position;
    }

    public String getTopic() {
        return this.topic;
    }

    public Integer getvGroupId() {return vGroupId;}
    public int hashCode() {
        if (this.hash != 0) {
            return this.hash;
        } else {
            return Objects.hash(topic, vGroupId);
        }
    }

    public String toString() {
        return this.topic + "-" + this.vGroupId + "-" + this.position;
    }
}
