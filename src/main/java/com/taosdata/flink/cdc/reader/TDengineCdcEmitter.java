package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.entity.CdcRecords;
import com.taosdata.flink.cdc.split.TDengineCdcSplitState;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.Iterator;

public class TDengineCdcEmitter<T> implements RecordEmitter<CdcRecords<T>, T, TDengineCdcSplitState> {
    private final boolean isBatchMode;
    public TDengineCdcEmitter(boolean isBatchMode) {
        this.isBatchMode = isBatchMode;
    }
    @Override
    public void emitRecord(CdcRecords<T> sourceRecords, SourceOutput<T> sourceOutput, TDengineCdcSplitState splitsState) throws Exception {
        splitsState.setTopicPartitions(sourceRecords.getPartitions());
        if (isBatchMode) {
            sourceOutput.collect((T) sourceRecords.getRecords());
        }else{
            Iterator<ConsumerRecord<T>> iterator = sourceRecords.getRecords().iterator();
            while (iterator.hasNext()) {
                sourceOutput.collect(iterator.next().value());
            }
        }

    }
}
