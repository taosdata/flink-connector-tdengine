package com.taosdata.flink.cdc.split;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;

import java.util.ArrayList;
import java.util.List;

public class TDengineCdcSplitState extends TDengineCdcSplit {
    private List<CdcTopicPartition> partitions;
    public TDengineCdcSplitState(String topic, String groupId, String clientId, List<CdcTopicPartition> partitions) {
        super(topic + "_" + groupId + "_"+ clientId, topic, groupId, clientId);
        this.partitions = partitions;
    }
    public TDengineCdcSplitState(TDengineCdcSplit cdcSplit) {
        super(cdcSplit.splitId, cdcSplit.getTopic(), cdcSplit.getGroupId(), cdcSplit.getClientId());
        this.partitions = new ArrayList<>();
    }
    public List<CdcTopicPartition> getTopicPartitions() {
        return partitions;
    }
    public void setTopicPartitions(List<CdcTopicPartition> partitions) {
        this.partitions = partitions;
    }

    public TDengineCdcSplit toTDengineCdcSplit() {
        return new TDengineCdcSplit(
                getTopic(),
                getGroupId(),
                getClientId(),
                partitions);
    }
}
