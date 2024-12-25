package com.taosdata.flink.table;

import com.taosdata.flink.sink.TDengineSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class TDengineTableSink implements DynamicTableSink {
    private final Properties properties;
    private final List<String> fieldNameList;

    private final Integer sinkParallelism;

    public TDengineTableSink(Properties properties, List<String> fieldNameList, Integer sinkParallelism) throws SQLException {
        this.properties = properties;
        this.sinkParallelism = sinkParallelism;
        this.fieldNameList = fieldNameList;
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