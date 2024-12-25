package com.taosdata.flink.source.entity;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.*;

public class TDengineSourceRecordsWithSplitsIds<OUT> implements RecordsWithSplitIds<SplitResultRecords<OUT>> {

    private String splitId;

    private List<SplitResultRecords<OUT>> recordsList;

    private Iterator<SplitResultRecords<OUT>> recordIterator;
    private List<String> finishedSplits;

    public TDengineSourceRecordsWithSplitsIds(
            @Nullable String splitId,
            @Nullable SplitResultRecords<OUT> records,
            List<String> finishedSplits) {
        if (records != null) {
            recordsList = new ArrayList<>(1);
            recordsList.add(records);
            this.recordIterator = recordsList.iterator();
        }
        if (finishedSplits != null) {
            this.finishedSplits = new ArrayList<>(finishedSplits);
        }
        this.splitId = splitId;
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
    public SplitResultRecords<OUT> nextRecordFromSplit() {
        if (recordIterator != null) {
            if (recordIterator.hasNext()) {
                return recordIterator.next();
            }
        }
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        Set<String> finishedSet = new HashSet<>();
        if (this.finishedSplits != null) {
            for (String splitId : finishedSplits) {
                finishedSet.add(splitId);
            }
        }
        return finishedSet;
    }

    public static TDengineSourceRecordsWithSplitsIds forRecords(
            final String splitId, SplitResultRecords records) {
        return new TDengineSourceRecordsWithSplitsIds(splitId, records, null);
    }

    public static TDengineSourceRecordsWithSplitsIds forFinishedSplit(List<String> finishedSplits) {
        return new TDengineSourceRecordsWithSplitsIds(null, null, finishedSplits);
    }
}
