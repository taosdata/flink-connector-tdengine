package com.taosdata.flink.source.entity;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class TdengineSourceRecords  implements RecordsWithSplitIds<SourceRecord> {

    @Nullable
    private String splitId;
    @Nullable private Iterator<SourceRecord> recordsForCurrentSplit;
    @Nullable private final Iterator<SourceRecord> recordsForSplit;
    private final Set<String> finishedSnapshotSplits;

    public TdengineSourceRecords(
            @Nullable String splitId,
            @Nullable SourceRecords records,
            Set<String> finishedSnapshotSplits) {
        this.splitId = splitId;
        this.recordsForSplit = records.iterator();
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
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
        return finishedSnapshotSplits;
    }
    public static TdengineSourceRecords forRecords(
            final String splitId, SourceRecords records) {
        return new TdengineSourceRecords(splitId, records, Collections.singleton(splitId));
    }
    public static TdengineSourceRecords forFinishedSplit(final String splitId) {
        return new TdengineSourceRecords(null, null, Collections.singleton(splitId));
    }
}
