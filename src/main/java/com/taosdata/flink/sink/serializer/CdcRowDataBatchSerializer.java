package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.taosdata.flink.sink.entity.DataType.DATA_TYPE_BINARY;

public class CdcRowDataBatchSerializer extends RowDataSerializerBase implements TDengineSinkRecordSerializer<ConsumerRecords<RowData>>{

    public CdcRowDataBatchSerializer() {
    }

    @Override
    public List<TDengineSinkRecord> serialize(ConsumerRecords<RowData> records, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IOException("serialize ConsumerRecords is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>();
        Iterator<ConsumerRecord<RowData>> iterator = records.iterator();
        while (iterator.hasNext()) {
            TDengineSinkRecord sinkRecord = getSinkRecord(iterator.next().value(), sinkMetaInfos);
            sinkRecords.add(sinkRecord);
        }
        return sinkRecords;
    }
}
