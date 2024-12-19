package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.entity.SplitResultRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class TDengineRowDataDeserialization implements TDengineRecordDeserialization<RowData> {
    /**
     * @param splitResultRecord A record of information containing an object list
     * @return Data format after data conversion
     * @throws SQLException
     */
    @Override
    public RowData convert(SplitResultRecord splitResultRecord) throws SQLException {
        List<Object> rowData = splitResultRecord.getSourceRecordList();
        for (int i = 0; i < rowData.size(); i++ ) {
            if (rowData.get(i) instanceof Timestamp) {
               rowData.set(i, TimestampData.fromTimestamp((Timestamp) rowData.get(i)));
            } else if (rowData.get(i) instanceof String) {
                rowData.set(i, StringData.fromString((String) rowData.get(i)));
            }
        }
        return GenericRowData.of(splitResultRecord.getSourceRecordList().toArray());
    }

    /**
     * @return Data Type after data conversion
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation.of(RowData.class));
    }
}
