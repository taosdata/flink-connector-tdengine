package com.taosdata.flink.source.entity;

import com.taosdata.flink.source.split.TDengineSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public class TdengineSourceRecords  implements RecordsWithSplitIds<SourceRecord> {

    @Nullable
    private String splitId;
    @Nullable private final Iterator<SourceRecord> recordsForSplit;
    private final List<TDengineSplit> assignmentSplits;
    private final List<TDengineSplit> finishedSplits;
    public TdengineSourceRecords(
            @Nullable String splitId,
            @Nullable SourceRecords records,
            List<TDengineSplit> currSplits,
            List<TDengineSplit> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = records.iterator();
        this.assignmentSplits = currSplits;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        if (this.assignmentSplits != null && !this.assignmentSplits.isEmpty()) {
            this.splitId = this.assignmentSplits.iterator().next().splitId();
        } else {
            this.splitId = null;
        }
        return this.splitId;
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        final Iterator<SourceRecord> recordsForSplit = this.recordsForSplit;
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
        finishedSplits.stream().map(split -> set.addAll(split.getFinishList()));
        return set;
    }

    public static TdengineSourceRecords forRecords(
            final String splitId, SourceRecords records, List<TDengineSplit> assignmentSplits,
            List<TDengineSplit> finishedSplits) {
        return new TdengineSourceRecords(splitId, records, assignmentSplits, finishedSplits);
    }
    public static TdengineSourceRecords forFinishedSplit(final String splitId, List<TDengineSplit> finishedSplits) {
        return new TdengineSourceRecords(null, new SourceRecords(), null, finishedSplits);
    }
}
