package com.taosdata.flink.table;

import com.google.common.base.Strings;
import com.taosdata.flink.sink.TDengineSink;
import com.taosdata.flink.sink.serializer.RowDataSinkRecordSerializer;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;

import java.sql.*;
import java.util.*;

public class TDengineTableSink implements DynamicTableSink {
    private final String dbName;
    private final String superTableName;

    private final String normalTableName;

    private final String url;

    private final Properties properties;

    private final List<SinkMetaInfo> sinkMetaInfos;
    private List<SinkMetaInfo> sinkSqlMetaInfos;
    private TDengineSinkRecordSerializer<RowData> serializer;
    private final Integer sinkParallelism;
    private final int batchSize;

    public TDengineTableSink(String dbName, String superTableName, String normalTableName, String url, Properties properties, TableSchema physicalSchema, Integer sinkParallelism, int batchSize) throws SQLException {
        this.dbName = dbName;
        this.superTableName = superTableName;
        this.normalTableName = normalTableName;
        this.url = url;
        this.properties = properties;
        this.sinkParallelism = sinkParallelism;
        this.batchSize = batchSize;
        sinkMetaInfos = new ArrayList<>();
        sinkSqlMetaInfos = new ArrayList<>();
        Map<String, SinkMetaInfo> metaInfoMap = null;
        if (!Strings.isNullOrEmpty(superTableName)) {
            metaInfoMap = getMetaInfo(superTableName);
        }else if (!Strings.isNullOrEmpty(normalTableName)) {
            metaInfoMap = getMetaInfo(normalTableName);
        }

        if (metaInfoMap != null) {
            List<TableColumn> tableColumns = physicalSchema.getTableColumns();
            for (TableColumn tableColumn : tableColumns) {
                SinkMetaInfo sinkMetaInfo = metaInfoMap.get(tableColumn.getName());
                if (sinkMetaInfo != null) {
                    sinkMetaInfos.add(sinkMetaInfo);
                    sinkSqlMetaInfos.add(sinkMetaInfo);
                } else if (tableColumn.getName().equals("tbname")) {
                    sinkMetaInfos.add(new SinkMetaInfo(false, DataType.DATA_TYPE_VARCHAR, "tbname", 192));
                }
            }
        }
        serializer = new RowDataSinkRecordSerializer(sinkMetaInfos);
    }

    public TDengineTableSink(TDengineTableSink tableSink) {
        this.dbName = tableSink.getDbName();
        this.superTableName = tableSink.superTableName;
        this.normalTableName = tableSink.normalTableName;
        this.url = tableSink.url;
        this.properties = tableSink.getProperties();
        this.sinkParallelism = tableSink.sinkParallelism;
        this.sinkMetaInfos = tableSink.sinkMetaInfos;
        this.serializer = tableSink.getSerializer();
        this.batchSize = tableSink.batchSize;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new TDengineSink<>(dbName, superTableName, normalTableName, url, properties, serializer, sinkSqlMetaInfos, 500), sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new TDengineTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "TDengine Table Sink";
    }

    public Map<String, SinkMetaInfo> getMetaInfo(String tableName) throws SQLException {
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

    public String getDbName() {
        return dbName;
    }

    public String getSuperTableName() {
        return superTableName;
    }

    public String getNormalTableName() {
        return normalTableName;
    }

    public String getUrl() {
        return url;
    }

    public Properties getProperties() {
        return properties;
    }

    public List<SinkMetaInfo> getSinkMetaInfos() {
        return sinkMetaInfos;
    }

    public TDengineSinkRecordSerializer<RowData> getSerializer() {
        return serializer;
    }

    public Integer getSinkParallelism() {
        return sinkParallelism;
    }



}