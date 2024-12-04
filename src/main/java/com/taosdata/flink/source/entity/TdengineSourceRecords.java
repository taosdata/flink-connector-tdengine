package com.taosdata.flink.source.entity;

import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.*;

public class TdengineSourceRecords  implements RecordsWithSplitIds<SourceRecord> {

    @Nullable
    private String splitId;
    @Nullable private final Iterator<SourceRecord> recordsForSplitItor;
    private final List<TDengineSplit> finishedSplits;
    public TdengineSourceRecords(
            @Nullable String splitId,
            @Nullable SourceRecords records,
            List<TDengineSplit> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplitItor = records.iterator();
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;
        return nextSplit;
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        final Iterator<SourceRecord> recordsForSplit = this.recordsForSplitItor;
        if (recordsForSplit != null) {
            if (recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        Set<String> set = new HashSet<>();
        if (this.finishedSplits != null) {
            for (TDengineSplit split : finishedSplits) {
                if (split != null) {
                    set.add(split.splitId());
                }

            }
        }
        return set;
    }

    public static TdengineSourceRecords forRecords(
            final String splitId, SourceRecords records, List<TDengineSplit> finishedSplits) {
        return new TdengineSourceRecords(splitId, records, null);
    }
    public static TdengineSourceRecords forFinishedSplit(final String splitId, List<TDengineSplit> finishedSplits) {
        return new TdengineSourceRecords(null, new SourceRecords(), finishedSplits);
    }
}
