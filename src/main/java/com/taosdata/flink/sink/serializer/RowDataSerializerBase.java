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
import java.util.List;

import static com.taosdata.flink.sink.entity.DataType.DATA_TYPE_BINARY;

public class RowDataSerializerBase {
    public TDengineSinkRecord getSinkRecord(RowData record, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>(1);
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
