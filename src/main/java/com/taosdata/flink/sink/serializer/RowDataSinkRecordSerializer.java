package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.taosdata.flink.sink.entity.DataType.DATA_TYPE_BINARY;

public class RowDataSinkRecordSerializer implements TDengineSinkRecordSerializer<RowData>{
    private final List<SinkMetaInfo> sinkMetaInfos;

    public RowDataSinkRecordSerializer(List<SinkMetaInfo> sinkMetaInfos) {
        this.sinkMetaInfos = sinkMetaInfos;
    }

    @Override
    public List<TDengineSinkRecord> serialize(RowData record) throws IOException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>(1);
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
        sinkRecords.add(new TDengineSinkRecord(tbname, tagParams, columnParams));
        return sinkRecords;
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
