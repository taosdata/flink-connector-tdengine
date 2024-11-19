package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.serializable.TdengineRecordDeserialization;
import com.taosdata.flink.source.split.TdengineSplit;
import com.taosdata.flink.source.entity.SourceRecord;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class TdengineRecordEmitter<T> implements RecordEmitter<SourceRecord, T, TdengineSplit> {
    private TdengineRecordDeserialization<T> tdengineRecordDeserialization;
    public TdengineRecordEmitter(TdengineRecordDeserialization<T> tdengineRecordDeserialization) {
        this.tdengineRecordDeserialization = tdengineRecordDeserialization;
    }


    @Override
    public void emitRecord(SourceRecord sourceRecord, SourceOutput<T> sourceOutput, TdengineSplit tdengineSplitsState) throws Exception {
        T data = this.tdengineRecordDeserialization.convert(sourceRecord, null);
        sourceOutput.collect(data);
    }
}
