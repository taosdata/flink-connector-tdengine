package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TDengineSplitSerializer implements SimpleVersionedSerializer<TDengineSplit> {
    private static final int CURRENT_VERSION = 0;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TDengineSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            List<String> taskList = split.gettasksplits();
            out.writeInt(taskList.size());
            for (String task : taskList) {
                out.writeUTF(task);
            }

            List<String> finishList = split.getFinishList();
            out.writeInt(finishList.size());
            for (String task : finishList) {
                out.writeUTF(task);
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public TDengineSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String splitId = in.readUTF();
            TDengineSplit tdSplit = new TDengineSplit(splitId);

            List<String> taskList = new ArrayList<>();
            int count = in.readInt();
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    String task = in.readUTF();
                    taskList.add(task);
                }
                tdSplit.setTaskSplits(taskList);
            }

            List<String> finishList = new ArrayList<>();
            count = in.readInt();
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    String task = in.readUTF();
                    finishList.add(task);
                }
                tdSplit.setFinishList(finishList);
            }
            return tdSplit;
        }
    }
}
