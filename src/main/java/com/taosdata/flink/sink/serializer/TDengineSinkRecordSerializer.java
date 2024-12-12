package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.TDengineSinkRecord;

import java.io.IOException;
import java.io.Serializable;

public interface TDengineSinkRecordSerializer<T> extends Serializable {
    TDengineSinkRecord serialize(T record) throws IOException;
}
