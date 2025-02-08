package com.taosdata.flink.table;

import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.source.TDengineSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
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

public class TDengineTableSource implements ScanTableSource {
    private String url;
    private Properties connProps;

    private DataType physicalDataType;
    private String scanQuery;
    private String mode;

    public TDengineTableSource(String url, String scanQuery, DataType physicalDataType, Properties connProps) {
        this.url = url;
        this.scanQuery = scanQuery;
        this.physicalDataType = physicalDataType;
        this.connProps = connProps;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        this.connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        this.connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, this.url);
        TDengineSource<RowData> tdengineSource = new TDengineSource<>(connProps, new SourceSplitSql(scanQuery), RowData.class);
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
            }

            @Override
            public boolean isBounded() {
                return tdengineSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new TDengineTableSource(this.url, this.scanQuery, this.physicalDataType, this.connProps);
    }

    @Override
    public String asSummaryString() {
        return "tdengine table source";
    }

}
