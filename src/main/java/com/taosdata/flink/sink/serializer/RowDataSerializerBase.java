package com.taosdata.flink.sink.serializer;

import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RowDataSerializerBase {
    private final Logger LOG = LoggerFactory.getLogger(RowDataSerializerBase.class);
    public TDengineSinkRecord getSinkRecord(RowData record, List<SinkMetaInfo> sinkMetaInfos) throws IOException, SQLException {
        if (record == null) {
            throw new IOException("serialize RowData is null!");
        }

        List<Object> columnParams = new ArrayList<>();
        for (int i = 0; i < sinkMetaInfos.size(); i++) {
            if (!record.isNullAt(i)) {
                switch (sinkMetaInfos.get(i).getFieldType()) {
                    case DATA_TYPE_BINARY:
                    case DATA_TYPE_VARCHAR:
                        Object binaryVal = new String(record.getBinary(i), StandardCharsets.UTF_8);
                        columnParams.add(binaryVal);
                        break;
                    case DATA_TYPE_INT:
                        Object intVal = record.getInt(i);
                        columnParams.add(intVal);
                        break;
                    case DATA_TYPE_BOOL:
                        Object bVal = record.getBoolean(i);
                        columnParams.add(bVal);
                        break;
                    case DATA_TYPE_FLOAT:
                        Object floatVal = record.getFloat(i);
                        columnParams.add(floatVal);
                        break;
                    case DATA_TYPE_DOUBLE:
                        Object doubleVal = record.getDouble(i);
                        columnParams.add(doubleVal);
                        break;
                    case DATA_TYPE_BIGINT:
                        Object longVal = record.getLong(i);
                        columnParams.add(longVal);
                        break;
                    case DATA_TYPE_TINYINT:
                        Object byteVal = record.getByte(i);
                        columnParams.add(byteVal);
                        break;
                    case DATA_TYPE_JSON:
                    case DATA_TYPE_NCHAR:
                        Object strVal = null;
                        if (record.getString(i) != null) {
                            strVal = record.getString(i).toString();
                        }
                        columnParams.add(strVal);
                        break;
                    case DATA_TYPE_VARBINARY:
                    case DATA_TYPE_GEOMETRY:
                        Object binary = record.getBinary(i);
                        columnParams.add(binary);
                        break;
                    case DATA_TYPE_SMALLINT:
                        Object shortVal = record.getShort(i);
                        columnParams.add(shortVal);
                        break;
                    case DATA_TYPE_TIMESTAMP:
                        Object timeVal = null;
                        if (record.getTimestamp(i, 5) != null) {
                            columnParams.add(record.getTimestamp(i, 5).toTimestamp());
                        } else {
                            columnParams.add(timeVal);
                        }
                        break;
                    default:
                        LOG.error("Unknown data type：" + sinkMetaInfos.get(i).getFieldType());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);

                }
            } else {
                columnParams.add(null);
            }
        }
        return new TDengineSinkRecord(columnParams);
    }

}
