package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.entity.SplitResultRecord;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.tmq.DeserializerException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class TDengineRowDataDeserialization implements TDengineRecordDeserialization<RowData> {
    private final Logger LOG = LoggerFactory.getLogger(TDengineRowDataDeserialization.class);
    /**
     * @param splitResultRecord A record of information containing an object list
     * @return Data format after data conversion
     * @throws SQLException
     */
    @Override
    public RowData convert(SplitResultRecord splitResultRecord) throws SQLException {
        List<Object> rowData = splitResultRecord.getSourceRecordList();
        BinaryRowData binaryRowData = new BinaryRowData(rowData.size());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeRowKind(RowKind.INSERT);
        ResultSetMetaData metaData = splitResultRecord.getMetaData();
        for (int i = 0; i < rowData.size(); i++) {
            Object value = rowData.get(i);
            if (value == null) {
                binaryRowWriter.setNullAt(i);
            } else {
                if (value instanceof Timestamp) {
                    // Convert Timestamp to the TimestampData type supported by RowData
                    binaryRowWriter.writeTimestamp(i , TimestampData.fromTimestamp((Timestamp) value), 5);
                } else if (value instanceof String) {
                    // Convert String to the StringData type supported by RowData
                    binaryRowWriter.writeString(i , StringData.fromString((String) value));
                } else if (value instanceof Byte) {
                    binaryRowWriter.writeByte(i , (Byte) value);
                } else if (value instanceof Integer) {
                    binaryRowWriter.writeInt(i , (Integer) value);
                } else if (value instanceof Boolean) {
                    binaryRowWriter.writeBoolean(i , (Boolean) value);
                } else if (value instanceof Float) {
                    binaryRowWriter.writeFloat(i, (Float) value);
                } else if (value instanceof Double) {
                    binaryRowWriter.writeDouble(i, (Double) value);
                } else if (value instanceof Long) {
                    binaryRowWriter.writeLong(i, (Long) value);
                } else if (value instanceof Short) {
                    binaryRowWriter.writeShort(i, (Short) value);
                } else if (value instanceof byte[]) {
                    if (metaData.getColumnTypeName(i + 1).equals("VARCHAR") || metaData.getColumnTypeName(i + 1).equals("BINARY")) {
                        String strVal = new String((byte[]) value, StandardCharsets.UTF_8);
                        binaryRowWriter.writeString(i, StringData.fromString(strVal));
                    }else {
                        binaryRowWriter.writeBinary(i, (byte[]) value);
                    }
                } else {
                    LOG.error("Unknown data type：" + value.getClass().getName());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);

                }
            }
        }
        return binaryRowData;
    }
    /**
     * @return Data Type after data conversion
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
