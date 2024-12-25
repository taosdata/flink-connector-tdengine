package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.flink.source.entity.SourceRecords;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SourceRowDataBatchSerializer extends RowDataSerializerBase implements TDengineSinkRecordSerializer<SourceRecords<RowData>>{

    public SourceRowDataBatchSerializer() {

    }

    @Override
    public List<TDengineSinkRecord> serialize(SourceRecords<RowData> records, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IOException("serialize SourceRecords is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>();
        Iterator<RowData> iterator = records.iterator();
        while (iterator.hasNext()) {
            TDengineSinkRecord sinkRecord = getSinkRecord(iterator.next(), sinkMetaInfos);
            sinkRecords.add(sinkRecord);
        }
        return sinkRecords;
    }
}
