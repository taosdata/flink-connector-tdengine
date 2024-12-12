/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taosdata.flink.source.entity;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Data structure to describe a set of T. */
public final class SourceRecords {

    private ResultSetMetaData metaData;
    private final List<SourceRecord> sourceRecords;

    public SourceRecords(ResultSetMetaData metaData, List<SourceRecord> sourceRecords) {
        this.metaData = metaData;
        this.sourceRecords = sourceRecords;
    }
    public SourceRecords() {
        this.sourceRecords = new ArrayList<>();
    }

    public void addSourceRecord(SourceRecord sourceRecord) {
        sourceRecords.add(sourceRecord);
    }

    public List<SourceRecord> getSourceRecordList() {
        return sourceRecords;
    }

    public Iterator<SourceRecord> iterator() {
        return sourceRecords.iterator();
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }
    public void setMetaData(ResultSetMetaData metaData) {this.metaData = metaData;}
}