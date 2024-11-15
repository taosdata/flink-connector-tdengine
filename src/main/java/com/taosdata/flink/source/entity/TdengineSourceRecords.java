package com.taosdata.flink.source.entity;

import com.taosdata.flink.source.TdengineSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public class TdengineSourceRecords  implements RecordsWithSplitIds<SourceRecord> {

    @Nullable
    private String splitId;
    @Nullable private Iterator<SourceRecord> recordsForCurrentSplit;
    @Nullable private final Iterator<SourceRecord> recordsForSplit;
    private final Deque<TdengineSplit> currSplits;
    private final Deque<TdengineSplit> finishedSplits;
    public TdengineSourceRecords(
            @Nullable String splitId,
            @Nullable SourceRecords records,
            Deque<TdengineSplit> currSplits,
            Deque<TdengineSplit> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = records.iterator();
        this.currSplits = currSplits;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        if (this.currSplits != null && !this.currSplits.isEmpty()) {
            return this.currSplits.iterator().next().getSql();
        }
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForCurrentSplit = nextSplit != null ? this.recordsForSplit : null;
        return nextSplit;
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        final Iterator<SourceRecord> recordsForSplit = this.recordsForCurrentSplit;
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
        return finishedSplits.stream().map(split -> split.getSql()).collect(Collectors.toSet());
    }
    public static TdengineSourceRecords forRecords(
            final String splitId, SourceRecords records,Deque<TdengineSplit> currSplits,
            Deque<TdengineSplit> finishedSplits) {
        return new TdengineSourceRecords(splitId, records, currSplits, finishedSplits);
    }
    public static TdengineSourceRecords forFinishedSplit(final String splitId, Deque<TdengineSplit> finishedSplits) {
        return new TdengineSourceRecords(null, new SourceRecords(), null, finishedSplits);
    }
}
