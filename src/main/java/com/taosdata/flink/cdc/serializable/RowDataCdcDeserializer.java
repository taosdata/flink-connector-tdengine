package com.taosdata.flink.cdc.serializable;

import com.taosdata.jdbc.tmq.Deserializer;
import com.taosdata.jdbc.tmq.DeserializerException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RowDataCdcDeserializer implements Deserializer<RowData>, ResultTypeQueryable<RowData> {
    @Override
    public RowData deserialize(ResultSet data, String topic, String dbName) throws DeserializerException, SQLException {
        ResultSetMetaData metaData = data.getMetaData();
        GenericRowData row = new GenericRowData(metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row.setField(i - 1, data.getObject(i));
        }
        return row;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
