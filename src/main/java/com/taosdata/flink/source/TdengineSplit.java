package com.taosdata.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

public class TdengineSplit implements SourceSplit {
    protected final String splitId;
    private final String sql;
    public TdengineSplit(String splitId, String sql) {
        this.splitId = splitId;
        this.sql = sql;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TdengineSplit that = (TdengineSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

}
