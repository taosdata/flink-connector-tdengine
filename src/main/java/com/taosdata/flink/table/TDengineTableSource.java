package com.taosdata.flink.table;

import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.flink.source.TdengineSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.table.options.JdbcLookupOptions;
import com.taosdata.flink.table.options.JdbcOptions;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;
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
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TdengineSource<RowData> tdengineSource = new TdengineSource<>(this.url, connProps, new SourceSplitSql(scanQuery), new TdengineRowDataDeserialization());
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
                return sourceStream;
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
