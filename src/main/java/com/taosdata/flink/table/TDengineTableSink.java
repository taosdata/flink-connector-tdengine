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
import java.util.stream.Collectors;

public class TDengineTableSink implements DynamicTableSink {
    private final Properties properties;
    private List<String> fieldNameList;

    private final Integer sinkParallelism;

    public TDengineTableSink(Properties properties, TableSchema physicalSchema, Integer sinkParallelism) throws SQLException {
        this.properties = properties;
        this.sinkParallelism = sinkParallelism;
        fieldNameList = physicalSchema.getTableColumns().stream().map(TableColumn::getName).collect(Collectors.toList());
    }

    public TDengineTableSink(TDengineTableSink tableSink) {
        this.properties = tableSink.getProperties();
        this.sinkParallelism = tableSink.sinkParallelism;
        this.fieldNameList = tableSink.fieldNameList;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        try {
            return SinkV2Provider.of(new TDengineSink<>( properties, fieldNameList), sinkParallelism);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new TDengineTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "TDengine Table Sink";
    }

    public Properties getProperties() {
        return properties;
    }


    public Integer getSinkParallelism() {
        return sinkParallelism;
    }



}