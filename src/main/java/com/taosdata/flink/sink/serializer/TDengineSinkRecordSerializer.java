package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface TDengineSinkRecordSerializer<T> extends Serializable {
    List<TDengineSinkRecord> serialize(T records, List<SinkMetaInfo> sinkMetaInfos) throws IOException, SQLException;
}
