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
import com.taosdata.flink.source.enumerator.TdengineSourceEnumState;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;


/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator
 * state of Kafka source.
 */
@Internal
public class TDengineSourceEnumStateSerializer
        implements SimpleVersionedSerializer<TdengineSourceEnumState> {

    /**
     * state of VERSION_0 contains splitAssignments, which is a mapping from subtask ids to lists of
     * assigned splits.
     */
    private static final int VERSION_0 = 0;
    /** state of VERSION_1 only contains assignedPartitions, which is a list of assigned splits. */
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
    public byte[] serialize(TdengineSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeBoolean(enumState.isInitFinished());
            List<String> taskList = enumState.getAssignmentSqls();
            out.writeInt(taskList.size());
            for (String task : taskList) {
                out.writeUTF(task);
            }

            Deque<String> finishList = enumState.getUnassignedSqls();
            out.writeInt(finishList.size());
            for (String task : finishList) {
                out.writeUTF(task);
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public TdengineSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            boolean isInitFinished = in.readBoolean();
            int count = in.readInt();
            List<String> assignmentSqls = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                assignmentSqls.add(in.readUTF());
            }

            count = in.readInt();
            Deque<String> unassignedSqls = new ArrayDeque<>(count);
            for (int i = 0; i < count; i++) {
                unassignedSqls.push(in.readUTF());
            }

            return new TdengineSourceEnumState(unassignedSqls, assignmentSqls, isInitFinished);
        }
    }

}
