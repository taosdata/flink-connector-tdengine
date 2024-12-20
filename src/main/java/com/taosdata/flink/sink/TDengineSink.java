package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkError;
import com.taosdata.flink.sink.entity.SinkErrorNumbers;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.serializer.CdcRowDataBatchSerializer;
import com.taosdata.flink.sink.serializer.RowDataSinkRecordSerializer;
import com.taosdata.flink.sink.serializer.SourceRowDataBatchSerializer;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.Utils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class TDengineSink<IN> implements Sink<IN> {
    private  String dbName;
    private  String superTableName;

    private  String normalTableName;

    private  String url;

    private  Properties properties;

    private  TDengineSinkRecordSerializer<IN> serializer;

    private  List<SinkMetaInfo> metaInfos;

    private  int batchSize;

    public TDengineSink(Properties properties, List<String> fieldNameList) throws SQLException {
        this.properties = properties;
        metaInfos = new ArrayList<>();
        initSinkParams(fieldNameList);
    }

    private void initSinkParams(List<String> fieldNameList) throws SQLException {

        this.url = properties.getProperty(TDengineConfigParams.TD_JDBC_URL);
        if (Strings.isNullOrEmpty(this.url)) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);
        }

        this.dbName = properties.getProperty(TDengineConfigParams.TD_DATABASE_NAME);
        if (Strings.isNullOrEmpty(this.dbName)) {
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
        }
        this.superTableName = properties.getProperty(TDengineConfigParams.TD_SUPERTABLE_NAME);
        this.normalTableName = properties.getProperty(TDengineConfigParams.TD_TABLE_NAME);
        if (Strings.isNullOrEmpty(this.superTableName) && Strings.isNullOrEmpty(this.normalTableName)) {
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_TABLE_NAME_NULL);
        }

        Map<String, SinkMetaInfo> sinkMetaInfoMap = getMetaInfo();
        for (String fieldName : fieldNameList) {
            if (fieldName.equals("tbname")) {
                metaInfos.add(new SinkMetaInfo(false, DataType.DATA_TYPE_VARCHAR, "tbname", 192));
            } else {
                SinkMetaInfo sinkMetaInfo = sinkMetaInfoMap.get(fieldName);
                if (sinkMetaInfo == null) {
                    throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_SINK_Field_NAME);
                }
                metaInfos.add(sinkMetaInfo);
            }
        }


        String strBatchSize = properties.getProperty(TDengineConfigParams.BATCH_SIZE, "1");
        this.batchSize = Integer.parseInt(strBatchSize);
        String sourceType = this.properties.getProperty(TDengineConfigParams.TD_SOURCE_TYPE, "");
        String batchMode = this.properties.getProperty(TDengineConfigParams.TD_BATCH_MODE, "false");
        String outType = this.properties.getProperty(TDengineConfigParams.VALUE_DESERIALIZER);
        if (Strings.isNullOrEmpty(outType)) {
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_VALUE_DESERIALIZER);
        }

        if (outType.equals("RowData")) {
            if (batchMode.equals("true")) {
                if (sourceType.equals("tdengine_source")) {
                    this.serializer = (TDengineSinkRecordSerializer<IN>) new SourceRowDataBatchSerializer(metaInfos);
                } else if (sourceType.equals("tdengine_cdc")) {
                    this.serializer = (TDengineSinkRecordSerializer<IN>) new CdcRowDataBatchSerializer(metaInfos);
                }
            } else {
                this.serializer = (TDengineSinkRecordSerializer<IN>) new RowDataSinkRecordSerializer(metaInfos);
            }
        }

        if (this.serializer == null) {
            this.serializer = (TDengineSinkRecordSerializer<IN>) Utils.newInstance(Utils.parseClassType(outType));
        }

    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        try {
            List<SinkMetaInfo> tagMetaInfos = new ArrayList<>();
            List<SinkMetaInfo> columnMetaInfos = new ArrayList<>();
            if (metaInfos != null) {
                for (SinkMetaInfo metaInfo : metaInfos) {
                    if (!metaInfo.getFieldName().equals("tbname")) {
                        if (metaInfo.isTag()) {
                            tagMetaInfos.add(metaInfo);
                        } else {
                            columnMetaInfos.add(metaInfo);
                        }
                    }
                }
                return new TDengineWriter<IN>(this.url, this.dbName, this.superTableName, this.normalTableName, this.properties, serializer, tagMetaInfos, columnMetaInfos, batchSize);
            }
            throw new SQLException("meta info is null!");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, SinkMetaInfo> getMetaInfo() throws SQLException {
        String tableName = superTableName;
        if (Strings.isNullOrEmpty(tableName)) {
            tableName = normalTableName;
        }

        Map<String, SinkMetaInfo> metaInfos = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(this.url, properties);
             Statement stmt = connection.createStatement()) {
            stmt.execute("describe `" + this.dbName + "`.`" + tableName + "`");
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                SinkMetaInfo metaInfo = new SinkMetaInfo();
                metaInfo.setFieldName(rs.getString(1));
                metaInfo.setFieldType(DataType.getDataType(rs.getString(2)));
                metaInfo.setLength(rs.getInt(3));
                metaInfo.setTag(rs.getString(4).equals("TAG"));
                metaInfos.put(metaInfo.getFieldName(), metaInfo);
            }
            return metaInfos;
        } catch (SQLException ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }
}
