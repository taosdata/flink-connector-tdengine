package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.TdengineSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class TDenginePartitionSplitSerializer  implements SimpleVersionedSerializer<TdengineSplit> {
    private static final int CURRENT_VERSION = 0;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TdengineSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getSql());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public TdengineSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String splitId = in.readUTF();
            String sql = in.readUTF();
            return new TdengineSplit(splitId, sql);
        }
    }
}
