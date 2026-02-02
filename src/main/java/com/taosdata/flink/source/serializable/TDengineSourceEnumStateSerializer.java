package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.enumerator.TDengineSourceEnumState;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.*;


/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator
 * state of source.
 */
@Internal
public class TDengineSourceEnumStateSerializer
        implements SimpleVersionedSerializer<TDengineSourceEnumState> {

    private static final int VERSION_1 = 1;

    private static final int CURRENT_VERSION = VERSION_1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TDengineSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeBoolean(enumState.isInitFinished());
            TreeSet<TDengineSplit> taskList = enumState.getAssignmentSqls();
            out.writeInt(taskList.size());
            for (TDengineSplit task : taskList) {
                out.writeUTF(task.splitId());
                out.writeInt(task.getTaskSplits().size());
                for (String split : task.getTaskSplits()) {
                    out.writeUTF(split);
                }
                out.writeInt(task.getFinishList().size());
                for (String split : task.getFinishList()) {
                    out.writeUTF(split);
                }
            }

            Deque<TDengineSplit> unassignedSplits = enumState.getUnassignedSqls();
            out.writeInt(unassignedSplits.size());
            for (TDengineSplit task : unassignedSplits) {
                out.writeUTF(task.splitId());
                out.writeInt(task.getTaskSplits().size());
                for (String split : task.getTaskSplits()) {
                    out.writeUTF(split);
                }
                out.writeInt(task.getFinishList().size());
                for (String split : task.getFinishList()) {
                    out.writeUTF(split);
                }
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public TDengineSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            boolean isInitFinished = in.readBoolean();
            int count = in.readInt();
            TreeSet<TDengineSplit> assignmentSplits = new TreeSet<>();
            for (int i = 0; i < count; i++) {
                TDengineSplit split = new TDengineSplit(in.readUTF());
                int taskCount = in.readInt();
                List<String> taskList = new ArrayList<>(taskCount);
                for (int j = 0; j < taskCount; j++) {
                    taskList.add(in.readUTF());
                }
                split.setTaskSplits(taskList);

                taskCount = in.readInt();
                List<String> finishList = new ArrayList<>(taskCount);
                for (int j = 0; j < taskCount; j++) {
                    finishList.add(in.readUTF());
                }
                split.setFinishList(finishList);
                assignmentSplits.add(split);
            }

            count = in.readInt();
            Deque<TDengineSplit> unassignedSplits = new ArrayDeque<>(count);
            for (int i = 0; i < count; i++) {
                TDengineSplit split = new TDengineSplit(in.readUTF());
                int taskCount = in.readInt();
                List<String> taskList = new ArrayList<>(taskCount);
                for (int j = 0; j < taskCount; j++) {
                    taskList.add(in.readUTF());
                }
                split.setTaskSplits(taskList);

                taskCount = in.readInt();
                List<String> finishList = new ArrayList<>(taskCount);
                for (int j = 0; j < taskCount; j++) {
                    finishList.add(in.readUTF());
                }
                split.setFinishList(finishList);
                unassignedSplits.add(split);
            }

            return new TDengineSourceEnumState(unassignedSplits, assignmentSplits, isInitFinished);
        }
    }

}
