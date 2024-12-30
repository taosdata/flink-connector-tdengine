package com.taosdata.flink.cdc.serializable;

import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.DeserializerException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RowDataCdcDeserializer implements Deserializer<RowData>, ResultTypeQueryable<RowData> {
    /**
     * Serialize ResultSet data to RowData
     * @param data poll data
     * @param topic  topic
     * @param dbName database name
     * @return RowData
     * @throws DeserializerException
     * @throws SQLException
     */
    @Override
    public RowData deserialize(ResultSet data, String topic, String dbName) throws DeserializerException, SQLException {
        ResultSetMetaData metaData = data.getMetaData();
        GenericRowData row = new GenericRowData(metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Object value = data.getObject(i);
            if (value instanceof Timestamp) {
                // Convert Timestamp to the TimestampData type supported by RowData
                row.setField(i , TimestampData.fromTimestamp((Timestamp) value));
            } else if (value instanceof String) {
                // Convert String to the StringData type supported by RowData
                row.setField(i , StringData.fromString((String) value));
            } else {
                row.setField(i , value);
            }
        }
        return row;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
