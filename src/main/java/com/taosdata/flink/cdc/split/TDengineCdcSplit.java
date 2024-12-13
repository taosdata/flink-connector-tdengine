package com.taosdata.flink.cdc.split;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;
import java.util.Objects;

public class TDengineCdcSplit implements SourceSplit, Comparable<TDengineCdcSplit> {
    protected final String splitId;
    private final String groupId;
    private final String clientId;
    private final String topic;

    private List<CdcTopicPartition> startPartitions;

    public TDengineCdcSplit(String splitId, String topic, String groupId, String clientId) {
        this.splitId = splitId;
        this.groupId = groupId;
        this.clientId = clientId;
        this.topic = topic;
    }

    public TDengineCdcSplit(String topic, String groupId, String clientId) {
        this.splitId = topic + "_" + groupId + "_" + clientId;
        this.groupId = groupId;
        this.clientId = clientId;
        this.topic = topic;
    }

    public TDengineCdcSplit(String topic, String groupId, String clientId, List<CdcTopicPartition> startPartitions) {
        this.splitId = topic + "_" + groupId + "_" + clientId;
        this.groupId = groupId;
        this.clientId = clientId;
        this.topic = topic;
        this.startPartitions = startPartitions;
    }


    @Override
    public String splitId() {
        return this.splitId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TDengineCdcSplit that = (TDengineCdcSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getClientId() {
        return clientId;
    }


    public String getTopic() {
        return topic;
    }

    public void setStartPartitions(List<CdcTopicPartition> startPartitions) {
        this.startPartitions = startPartitions;
    }

    public List<CdcTopicPartition> getStartPartitions() {
        return startPartitions;
    }

    @Override
    public int compareTo(TDengineCdcSplit o) {
        return o.splitId.compareTo(this.splitId);
    }
}
