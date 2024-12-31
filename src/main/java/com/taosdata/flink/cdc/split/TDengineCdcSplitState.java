package com.taosdata.flink.cdc.split;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;

import java.util.ArrayList;
import java.util.List;

/**
 * Save the offset of each vgoup for the current consumer
 * topic groupId clientId partitions
 */
public class TDengineCdcSplitState extends TDengineCdcSplit {

    /**
     * @param topic
     * @param groupId
     * @param clientId
     * @param partitions
     */
    public TDengineCdcSplitState(String topic, String groupId, String clientId, List<CdcTopicPartition> partitions) {
        super(topic, groupId, clientId, partitions);

    }
    public TDengineCdcSplitState(TDengineCdcSplit cdcSplit) {
        super(cdcSplit.getTopic(), cdcSplit.getGroupId(), cdcSplit.getClientId(), cdcSplit.getStartPartitions());
    }

    public TDengineCdcSplit toTDengineCdcSplit() {
        return new TDengineCdcSplit(
                getTopic(),
                getGroupId(),
                getClientId(),
                getStartPartitions());
    }
}
