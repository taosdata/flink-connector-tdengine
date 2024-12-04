package com.taosdata.flink.cdc.entity;

import com.taosdata.jdbc.tmq.ConsumerRecords;

import java.util.List;

public class CdcRecord<OUT> {
    private final OUT record;
    private final List<CdcTopicPartition> partitions;


    public CdcRecord(OUT record, List<CdcTopicPartition> partitions) {
        this.record = record;
        this.partitions = partitions;
    }

    public OUT getRecord() {
        return record;
    }

    public List<CdcTopicPartition> getPartitions() {
        return partitions;
    }
}
