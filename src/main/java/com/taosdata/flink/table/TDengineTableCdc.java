package com.taosdata.flink.table;

import com.taosdata.flink.cdc.TDengineCdcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Properties;

public class TDengineTableCdc implements ScanTableSource {
    private String topic;
    private Properties properties;

    private DataType physicalDataType;
    private String scanQuery;

    public TDengineTableCdc(String topic, Properties properties) {
        this.topic = topic;
        this.properties = properties;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TDengineCdcSource<RowData> cdcSource = new TDengineCdcSource<>(this.topic, this.properties, RowData.class);
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "tdengine-cdc");
            }

            @Override
            public boolean isBounded() {
                return cdcSource.getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new TDengineTableCdc(this.topic, this.properties);
    }

    @Override
    public String asSummaryString() {
        return "tdengine table cdc";
    }

}