/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.enumerator.TDengineSourceEnumState;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.*;


/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator
 * state of Kafka source.
 */
@Internal
public class TDengineSourceEnumStateSerializer
        implements SimpleVersionedSerializer<TDengineSourceEnumState> {

    /**
     * state of VERSION_0 contains splitAssignments, which is a mapping from subtask ids to lists of
     * assigned splits.
     */
    private static final int VERSION_0 = 0;
    /**
     * state of VERSION_1 only contains assignedPartitions, which is a list of assigned splits.
     */
    private static final int VERSION_1 = 1;
    /**
     * state of VERSION_2 contains initialDiscoveryFinished and partitions with different assignment
     * status.
     */
    private static final int VERSION_2 = 2;

    private static final int CURRENT_VERSION = VERSION_2;

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
                out.writeInt(task.gettasksplits().size());
                for (String split : task.gettasksplits()) {
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
                out.writeInt(task.gettasksplits().size());
                for (String split : task.gettasksplits()) {
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
