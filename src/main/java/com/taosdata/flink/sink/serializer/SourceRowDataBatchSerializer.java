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
    private final List<SinkMetaInfo> sinkMetaInfos;

    public SourceRowDataBatchSerializer(List<SinkMetaInfo> sinkMetaInfos) {
        this.sinkMetaInfos = sinkMetaInfos;
    }

    @Override
    public List<TDengineSinkRecord> serialize(SourceRecords<RowData> records) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IOException("serialize SourceRecords is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>();
        Iterator<RowData> iterator = records.iterator();
        while (iterator.hasNext()) {
            TDengineSinkRecord sinkRecord = getSinkRecord(iterator.next());
            sinkRecords.add(sinkRecord);
        }
        return sinkRecords;
    }

    private TDengineSinkRecord getSinkRecord(RowData record) throws IOException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }

        GenericRowData rowData = (GenericRowData) record;
        List<Object> tagParams = new ArrayList<>();
        List<Object> columnParams = new ArrayList<>();
        String tbname = "";
        for (int i = 0; i < sinkMetaInfos.size(); i++) {
            Object value = rowData.getField(i);
            if (sinkMetaInfos.get(i).getFieldName().equals("tbname")) {
                if (value instanceof StringData) {
                    tbname = value.toString();
                }else if (value instanceof byte[]) {
                    tbname = new String((byte[]) value, StandardCharsets.UTF_8);
                }
            } else {
                Object fieldVal = convertRowDataType(value, sinkMetaInfos.get(i).getFieldType());
                if (sinkMetaInfos.get(i).isTag()) {
                    tagParams.add(fieldVal);
                }else{
                    columnParams.add(fieldVal);
                }
            }
        }

        return new TDengineSinkRecord(tbname, tagParams, columnParams);
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
