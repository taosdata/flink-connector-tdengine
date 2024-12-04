package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.serializable.TdengineRecordDeserialization;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.entity.SourceRecord;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class TdengineRecordEmitter<T> implements RecordEmitter<SourceRecord, T, TDengineSplitsState> {
    private TdengineRecordDeserialization<T> tdengineRecordDeserialization;
    public TdengineRecordEmitter(TdengineRecordDeserialization<T> tdengineRecordDeserialization) {
        this.tdengineRecordDeserialization = tdengineRecordDeserialization;
    }

    @Override
    public void emitRecord(SourceRecord sourceRecord, SourceOutput<T> sourceOutput, TDengineSplitsState splitsState) throws Exception {
        splitsState.setFinishList(sourceRecord.getFinishTaskList());
        T data = this.tdengineRecordDeserialization.convert(sourceRecord);
        sourceOutput.collect(data);
    }
}
