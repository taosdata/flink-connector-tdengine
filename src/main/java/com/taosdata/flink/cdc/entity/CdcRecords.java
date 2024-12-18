package com.taosdata.flink.cdc.entity;

import com.taosdata.jdbc.tmq.ConsumerRecords;

import java.util.List;

public class CdcRecords<OUT>{
    private final ConsumerRecords<OUT> records;
    private final List<CdcTopicPartition> partitions;

    public CdcRecords(ConsumerRecords<OUT> records, List<CdcTopicPartition> partitions) {
        this.records = records;
        this.partitions = partitions;
    }

    public ConsumerRecords<OUT> getRecords() {
        return records;
    }

    public List<CdcTopicPartition> getPartitions() {
        return partitions;
    }
}
