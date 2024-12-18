package com.taosdata.flink.cdc.split;

import com.taosdata.flink.cdc.entity.CdcRecord;
import com.taosdata.flink.cdc.entity.CdcRecords;
import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class TDengineRecordsWithSplitIds<OUT> implements RecordsWithSplitIds<CdcRecords<OUT>> {

    private List<CdcRecords<OUT>> cdcRecordsList;

    private Iterator<CdcRecords<OUT>> recordIterator;
    private List<CdcTopicPartition> topicPartitions;

    private String splitId;

    public TDengineRecordsWithSplitIds(String splitId, ConsumerRecords<OUT> consumerRecords, List<CdcTopicPartition> topicPartitions) {
        if (!consumerRecords.isEmpty()) {
            CdcRecords<OUT> cdcRecords = new CdcRecords<>(consumerRecords, topicPartitions);
            this.cdcRecordsList = new ArrayList<>(1);
            this.cdcRecordsList.add(cdcRecords);
            this.recordIterator = cdcRecordsList.iterator();
            this.topicPartitions = topicPartitions;
        }

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
    public CdcRecords<OUT> nextRecordFromSplit() {
        if (recordIterator != null) {
            if (recordIterator.hasNext()) {
                return recordIterator.next();
            }
        }
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        return new HashSet<>();
    }
}
