package com.taosdata.flink.table;

import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.flink.source.TdengineSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.table.options.JdbcLookupOptions;
import com.taosdata.flink.table.options.JdbcOptions;
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
import org.apache.flink.table.data.RowData;

import java.util.Properties;

public class TDengineTableSource implements ScanTableSource, SupportsProjectionPushDown {
    private String url;
    private Properties connProps;
    private String scanQuery;
    public TDengineTableSource(JdbcOptions connectOptions,
                               JdbcReadOptions readOptions,
                               JdbcLookupOptions lookupOptions,
                               TableSchema physicalSchema) {
        url = connectOptions.getDbURL();
        scanQuery = readOptions.getQuery().get();

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
//        final TDengineTableSource copy = new TDengineTableSource(this.url, this.connProps, this.scanQuery, null);
//        return copy;
        return null;
    }

    @Override
    public String asSummaryString() {
        return  "tdengine table source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }
}
