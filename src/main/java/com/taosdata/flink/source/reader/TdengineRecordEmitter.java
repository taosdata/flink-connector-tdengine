package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.entity.SplitResultRecords;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.Iterator;

public class TdengineRecordEmitter<T> implements RecordEmitter<SplitResultRecords<T>, T, TDengineSplitsState> {
    private final boolean isBatchMode;

    public TdengineRecordEmitter(boolean isBatchMode) {
        this.isBatchMode = isBatchMode;
    }

    @Override
    public void emitRecord(SplitResultRecords<T> splitResultRecords, SourceOutput<T> sourceOutput, TDengineSplitsState splitsState) throws Exception {
        splitsState.updateSplitsState(splitResultRecords.getTdengineSplit());
        if (isBatchMode) {
            sourceOutput.collect((T) splitResultRecords.getSourceRecords());
        } else {
            Iterator<T> iterator = splitResultRecords.iterator();
            while (iterator.hasNext()) {
                sourceOutput.collect(iterator.next());
            }
        }
    }
}
