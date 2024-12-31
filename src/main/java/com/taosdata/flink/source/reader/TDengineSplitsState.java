package com.taosdata.flink.source.reader;

import com.taosdata.flink.source.split.TDengineSplit;

public class TDengineSplitsState extends TDengineSplit {
    public TDengineSplitsState(String splitId) {
        super(splitId);
    }

    public TDengineSplitsState(TDengineSplit split) {
        super(split);
    }

    public void updateSplitsState(TDengineSplit split) {
        updateSplit(split);
    }

    public TDengineSplit toTDengineCdcSplit() {
        return this;
    }

}
