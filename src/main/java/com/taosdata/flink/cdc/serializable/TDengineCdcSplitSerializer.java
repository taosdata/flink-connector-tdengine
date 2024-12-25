package com.taosdata.flink.cdc.serializable;

import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TDengineCdcSplitSerializer implements SimpleVersionedSerializer<TDengineCdcSplit> {
    private static final int CURRENT_VERSION = 0;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TDengineCdcSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getTopic());
            out.writeUTF(split.getGroupId());
            out.writeUTF(split.getClientId());

            List<CdcTopicPartition> partitions = split.getStartPartitions();
            if (partitions != null && !partitions.isEmpty()) {
                out.writeInt(partitions.size());
                for (CdcTopicPartition partition : partitions) {
                    out.writeLong(partition.getPartition());
                    out.writeInt(partition.getvGroupId());

                }
            } else {
                out.writeInt(0);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public TDengineCdcSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String splitId = in.readUTF();
            String topic = in.readUTF();
            String groupId = in.readUTF();
            String clientId = in.readUTF();
            List<CdcTopicPartition> partitions = new ArrayList<>();
            int count = in.readInt();
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    CdcTopicPartition partition = new CdcTopicPartition(topic, in.readLong(), in.readInt());
                    partitions.add(partition);
                }
            }
            return new TDengineCdcSplit(topic, groupId, clientId, partitions);
        }
    }
}
