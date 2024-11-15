package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class TdengineRowDataDeserialization implements TdengineRecordDeserialization<RowData> {
    /**
     * @param sourceRecord A record of information containing an object list
     * @param metaData Meta information
     * @return Data format after data conversion
     * @throws SQLException
     */
    @Override
    public RowData convert(SourceRecord sourceRecord, ResultSetMetaData metaData) throws SQLException {
        return GenericRowData.of(sourceRecord.getSourceRecordList().toArray());
    }

    /**
     * @return Data Type after data conversion
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
