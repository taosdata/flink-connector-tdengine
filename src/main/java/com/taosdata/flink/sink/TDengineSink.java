package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.common.VersionComparator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class TDengineSink<IN> implements Sink<IN> {
    private final Logger LOG = LoggerFactory.getLogger(TDengineSink.class);
    private String dbName;
    private String superTableName;

    private String normalTableName;

    private String url;
    private String tdVersion;
    private String stmt2Version;
    private Properties properties;

    private TDengineSinkRecordSerializer<IN> serializer;

    private List<SinkMetaInfo> metaInfos;

    private int batchSize;

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
        this.stmt2Version = properties.getProperty(TDengineConfigParams.TD_STMT2_VERSION, "3.3.5.0");
        // Based on the field list provided by the user, determine the type of each field.
        // If the tbname field is specified, additional processing is required here.
        Map<String, SinkMetaInfo> sinkMetaInfoMap = getMetaInfo();
        for (String fieldName : fieldNameList) {
            if (fieldName.equals("tbname")) {
                metaInfos.add(new SinkMetaInfo(false, DataType.DATA_TYPE_VARCHAR, "tbname", 192));
            } else {
                SinkMetaInfo sinkMetaInfo = sinkMetaInfoMap.get(fieldName);
                if (sinkMetaInfo == null) {
                    LOG.error("The field name does not exist in the meta information of the table! fieldName:{}, dbname:{}, superTableName:{}, tableName:{}",
                            fieldName, this.dbName, this.superTableName, this.normalTableName);
                    throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_SINK_Field_NAME);
                }
                metaInfos.add(sinkMetaInfo);
            }
        }


        String strBatchSize = properties.getProperty(TDengineConfigParams.TD_BATCH_SIZE, "1");
        this.batchSize = Integer.parseInt(strBatchSize);
        String sourceType = this.properties.getProperty(TDengineConfigParams.TD_SOURCE_TYPE, "");
        String batchMode = this.properties.getProperty(TDengineConfigParams.TD_BATCH_MODE, "false");
        String outType = this.properties.getProperty(TDengineConfigParams.VALUE_DESERIALIZER);
        if (Strings.isNullOrEmpty(outType)) {
            LOG.error("value.deserializer parameter not set!");
            throw SinkError.createSQLException(SinkErrorNumbers.ERROR_INVALID_VALUE_DESERIALIZER);
        }

        if (outType.equals("RowData")) {
            // If it is batchMode, the data source type needs to be set
            if (batchMode.equals("true")) {
                if (sourceType.equals("tdengine_source")) {
                    this.serializer = (TDengineSinkRecordSerializer<IN>) new SourceRowDataBatchSerializer();
                } else if (sourceType.equals("tdengine_cdc")) {
                    this.serializer = (TDengineSinkRecordSerializer<IN>) new CdcRowDataBatchSerializer();
                }
            } else {
                this.serializer = (TDengineSinkRecordSerializer<IN>) new RowDataSinkRecordSerializer();
            }
        }

        if (this.serializer == null) {
            this.serializer = (TDengineSinkRecordSerializer<IN>) Utils.newInstance(Utils.parseClassType(outType));
        }
        LOG.debug("TDengineSink properties:{}", this.properties.toString());
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        try {
            if (metaInfos != null) {
                if (VersionComparator.compareVersion(tdVersion, stmt2Version) < 0) {
                    LOG.info("createWriter TDengine version not supported stmt2, create sql writer!");
                    return new TDengineSqlWriter<IN>(this.url, this.dbName, this.superTableName,
                            this.normalTableName, this.properties, serializer, metaInfos);
                }
                return new TDengineStmtWriter<>(this.url, this.dbName, this.superTableName,
                        this.normalTableName, this.properties, serializer, metaInfos, batchSize);
            }

            LOG.error("createWriter meta info is null!");
            throw new SQLException("meta info is null!");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get table meta info
     */
    public Map<String, SinkMetaInfo> getMetaInfo() throws SQLException {
        String tableName = superTableName;
        if (Strings.isNullOrEmpty(tableName)) {
            tableName = normalTableName;
        }

        Map<String, SinkMetaInfo> metaInfos = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(this.url, properties);
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT SERVER_VERSION()");
            ResultSet rs = statement.getResultSet();
            if (rs.next()) {
                tdVersion = rs.getString(1);
            }

            statement.execute("describe `" + this.dbName + "`.`" + tableName + "`");
            rs = statement.getResultSet();
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
            LOG.error("Failed to create database power or stable meters, dbname:{}, tableName:{}, ErrCode:{}, ErrMessage:{}",
                    this.dbName, tableName, ex.getErrorCode(), ex.getMessage());
            throw ex;
        }
    }
}
