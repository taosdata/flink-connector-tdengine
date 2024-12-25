package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowDataSinkRecordSerializer extends RowDataSerializerBase implements TDengineSinkRecordSerializer<RowData>{

    public RowDataSinkRecordSerializer() {
    }

    @Override
    public List<TDengineSinkRecord> serialize(RowData record, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>(1);
        TDengineSinkRecord sinkRecord = getSinkRecord(record, sinkMetaInfos);
        sinkRecords.add(sinkRecord);
        return sinkRecords;
    }
}
