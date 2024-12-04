package com.taosdata.flink.source.reader;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.source.split.TDengineSplit;

import java.util.List;

public class TDengineSplitsState extends TDengineSplit {
    public TDengineSplitsState(String splitId, List<String> finishTaskList) {
        super(splitId);
    }

    public TDengineSplitsState(TDengineSplit split) {
        super(split.splitId(), split.gettasksplits(), split.getFinishList());
    }

    public TDengineSplit toTDengineCdcSplit() {
        return this;
    }
}
