package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;


public class TDFlinkSourceTest {
    private static final int PARALLELISM = 2;
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;

    @BeforeEach
    void beforeEach() throws Exception {
        reporter = InMemoryReporter.create();
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .setConfiguration(
                                        reporter.addToConfiguration(new Configuration()))
                                .build());
        miniClusterResource.before();
    }

    @AfterEach
    void afterEach() {
        reporter.close();
        miniClusterResource.after();
    }
    @Test
    void testBasicMultiClusterRead() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        AtomicLong nCount = new AtomicLong();
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TdengineSource<RowData> source = new TdengineSource<>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata", connProps, sql, new TdengineRowDataDeserialization());
        DataStreamSource<RowData> input =env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("ts: " + row.getField(0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", location: " + new String(row.getBinary(4)));
            sb.append("\n");
            nCount.getAndIncrement();
            System.out.println(nCount.get());
            return sb.toString();
        });

        env.execute("flink tdengine source");
    }


}
