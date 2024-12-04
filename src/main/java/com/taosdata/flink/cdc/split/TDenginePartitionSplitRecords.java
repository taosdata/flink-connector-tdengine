package com.taosdata.flink.cdc.split;

import com.taosdata.flink.cdc.entity.CdcRecord;
import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TDenginePartitionSplitRecords<OUT> implements RecordsWithSplitIds<CdcRecord<OUT>> {
    private final Set<String> finishedSplits = new HashSet<>();

    private ConsumerRecords<OUT> records;

    private Iterator<CdcTopicPartition> splitIterator;
    private Iterator<ConsumerRecord<OUT>> recordIterator;
    private List<CdcTopicPartition> topicPartitions;

    private String splitId;

    public TDenginePartitionSplitRecords(String splitId, ConsumerRecords<OUT> records, List<CdcTopicPartition> topicPartitions) {
        this.records = records;
        this.recordIterator = records.iterator();
        this.topicPartitions = topicPartitions;
        this.splitId = splitId;
    }

    @Nullable
    @Override
    public String nextSplit() {
        final String nextSplit = this.splitId;
        this.splitId = null;
        return nextSplit;
    }

    @Nullable
    @Override
    public CdcRecord<OUT> nextRecordFromSplit() {
        if (recordIterator != null) {
            if (recordIterator.hasNext()) {
                OUT val = recordIterator.next().value();
                return new CdcRecord(val, topicPartitions);
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return new HashSet<>();
    }
}
