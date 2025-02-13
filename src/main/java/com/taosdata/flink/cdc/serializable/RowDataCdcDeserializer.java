package com.taosdata.flink.cdc.serializable;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.DeserializerException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RowDataCdcDeserializer implements Deserializer<RowData>, ResultTypeQueryable<RowData> {
    private final Logger LOG = LoggerFactory.getLogger(RowDataCdcDeserializer.class);
    /**
     * Serialize ResultSet data to RowData
     *
     * @param data   poll data
     * @param topic  topic
     * @param dbName database name
     * @return RowData
     * @throws DeserializerException
     * @throws SQLException
     */
    @Override
    public RowData deserialize(ResultSet data, String topic, String dbName) throws DeserializerException, SQLException {
        ResultSetMetaData metaData = data.getMetaData();
        BinaryRowData binaryRowData = new BinaryRowData(metaData.getColumnCount());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeRowKind(RowKind.INSERT);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Object value = data.getObject(i);
            if (value == null) {
                binaryRowWriter.setNullAt(i - 1);
            } else {
                if (value instanceof Timestamp) {
                    // Convert Timestamp to the TimestampData type supported by RowData
                    binaryRowWriter.writeTimestamp(i - 1, TimestampData.fromTimestamp((Timestamp) value), 5);
                } else if (value instanceof String) {
                    // Convert String to the StringData type supported by RowData
                    binaryRowWriter.writeString(i - 1, StringData.fromString((String) value));
                } else if (value instanceof Byte) {
                    binaryRowWriter.writeByte(i - 1, (Byte) value);
                } else if (value instanceof Integer) {
                    binaryRowWriter.writeInt(i - 1, (Integer) value);
                } else if (value instanceof Boolean) {
                    binaryRowWriter.writeBoolean(i - 1, (Boolean) value);
                } else if (value instanceof Float) {
                    binaryRowWriter.writeFloat(i - 1, (Float) value);
                } else if (value instanceof Double) {
                    binaryRowWriter.writeDouble(i - 1, (Double) value);
                } else if (value instanceof Long) {
                    binaryRowWriter.writeLong(i - 1, (Long) value);
                } else if (value instanceof Short) {
                    binaryRowWriter.writeShort(i - 1, (Short) value);
                } else if (value instanceof byte[]) {
                    if (metaData.getColumnTypeName(i - 1).equals("VARCHAR") || metaData.getColumnTypeName(i - 1).equals("BINARY")) {
                        String strVal = new String((byte[]) value, StandardCharsets.UTF_8);
                        binaryRowWriter.writeString(i - 1, StringData.fromString(strVal));
                    }else {
                        binaryRowWriter.writeBinary(i - 1, (byte[]) value);
                    }

                } else {
                    LOG.error("Unknown data type：" + value.getClass().getName());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);

                }
            }
        }
        return binaryRowData;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
