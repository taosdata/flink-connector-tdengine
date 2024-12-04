package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.entity.CdcRecord;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitState;
import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.flink.source.serializable.TdengineRecordDeserialization;
import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class TDengineCdcEmitter<T> implements RecordEmitter<CdcRecord<T>, T, TDengineCdcSplitState> {
    public TDengineCdcEmitter() {

    }
    @Override
    public void emitRecord(CdcRecord<T> sourceRecord, SourceOutput<T> sourceOutput, TDengineCdcSplitState splitsState) throws Exception {
        splitsState.setTopicPartitions(sourceRecord.getPartitions());
        sourceOutput.collect(sourceRecord.getRecord());
    }
}
