package com.taosdata.flink.source;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;


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
    void testTDengineSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TdengineSource<RowData> source = new TdengineSource<>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata", connProps, sql, new TdengineRowDataDeserialization());
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("ts: " + row.getField(0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", location: " + new String(row.getBinary(4)));
            sb.append("\n");
            System.out.println(sb);
            return sb.toString();
        });

        env.execute("flink tdengine source");
    }

    @Test
    void testTDengineCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(200, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties config = new Properties();
        config.setProperty("td.connect.type", "ws");
        config.setProperty("bootstrap.servers", "192.168.1.98:6041");
        config.setProperty("auto.offset.reset", "earliest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group_1");
        config.setProperty("td.connect.user", "root");
        config.setProperty("td.connect.pass", "taosdata");
        config.setProperty("value.deserializer", "RowData");
        config.setProperty("value.deserializer.encoding", "UTF-8");
        TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("tsxx: " + row.getField(0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", location: " + new String(row.getBinary(4)));
            sb.append("\n");
            System.out.println(sb);
            return sb.toString();
        });
        resultStream.addSink(new DiscardingSink<String>());
        env.execute("Flink test cdc Example");
    }
}
