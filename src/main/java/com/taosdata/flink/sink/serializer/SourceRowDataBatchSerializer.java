package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.flink.source.entity.SourceRecords;
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

public class SourceRowDataBatchSerializer implements TDengineSinkRecordSerializer<SourceRecords<RowData>>{

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

    private TDengineSinkRecord getSinkRecord(RowData record, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }

        GenericRowData rowData = (GenericRowData) record;
        List<Object> columnParams = new ArrayList<>();
        for (int i = 0; i < sinkMetaInfos.size(); i++) {
            Object fieldVal = convertRowDataType(rowData.getField(i), sinkMetaInfos.get(i).getFieldType());
            columnParams.add(fieldVal);
        }

        return new TDengineSinkRecord(columnParams);
    }
    private Object convertRowDataType(Object value, DataType fieldType) {
        if (value == null) {
            return null;
        }
        if (value instanceof TimestampData) {
            TimestampData timestampData = (TimestampData)value;
            return timestampData.toTimestamp();
        }
        if (value instanceof StringData) {
            StringData stringData = (StringData) value;
            return stringData.toString();
        }

        if (fieldType.getTypeNo() == DATA_TYPE_BINARY.getTypeNo()) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }

        return  value;
    }
}
