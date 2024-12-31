package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.entity.SplitResultRecords;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.Iterator;

public class TDengineRecordEmitter<T> implements RecordEmitter<SplitResultRecords<T>, T, TDengineSplitsState> {
    private final boolean isBatchMode;

    public TDengineRecordEmitter(boolean isBatchMode) {
        this.isBatchMode = isBatchMode;
    }

    @Override
    public void emitRecord(SplitResultRecords<T> splitResultRecords, SourceOutput<T> sourceOutput, TDengineSplitsState splitsState) throws Exception {
        // Batch mode requires the output type to be RecordEmitter<T>
        if (isBatchMode) {
            sourceOutput.collect((T) splitResultRecords.getSourceRecords());
        } else {
            // Single mode issuance
            Iterator<T> iterator = splitResultRecords.iterator();
            while (iterator.hasNext()) {
                sourceOutput.collect(iterator.next());
            }
        }
        // After the data distribution is completed, update the slice status so that it can be stored at the checkpoint
        splitsState.updateSplitsState(splitResultRecords.getTdengineSplit());
    }
}
