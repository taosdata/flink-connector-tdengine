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

import com.taosdata.flink.source.split.TDengineSplit;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Data structure to describe a set of T. */
public final class SplitResultRecords<V> {

    private ResultSetMetaData metaData;
    private SourceRecords<V> sourceRecords;
    private TDengineSplit tdengineSplit;
    public SplitResultRecords(ResultSetMetaData metaData, SourceRecords sourceRecords, TDengineSplit tdengineSplit) {
        this.metaData = metaData;
        this.sourceRecords = sourceRecords;
        this.tdengineSplit = new TDengineSplit(tdengineSplit);
    }
    public SplitResultRecords() {
    }

    public void setSourceRecords(SourceRecords sourceRecords) {
         this.sourceRecords = sourceRecords;
    }
    public SourceRecords getSourceRecords() {
        return sourceRecords;
    }

    public Iterator<V> iterator() {
        return sourceRecords.iterator();
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }
    public void setMetaData(ResultSetMetaData metaData) {this.metaData = metaData;}


    public TDengineSplit getTdengineSplit() {
        return tdengineSplit;
    }

    public void setTdengineSplit(TDengineSplit tdengineSplit) {
        this.tdengineSplit = new TDengineSplit(tdengineSplit);
    }
}
