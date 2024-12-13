package com.taosdata.flink.source;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
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
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        try (Connection connection = DriverManager.getConnection("jdbc:TAOS-RS://192.168.1.98:7041/power?user=root&password=taosdata", connProps);
             Statement stmt = connection.createStatement()) {
            stmt.execute("describe power.meters");
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                rs.getString(1);
                rs.getString(2);
                rs.getInt(3);
                rs.getString(4);
            }
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TdengineSource<RowData> source = new TdengineSource<>("jdbc:TAOS-RS://192.168.1.98:7041/power?user=root&password=taosdata", connProps, sql, new TdengineRowDataDeserialization());
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
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties config = new Properties();
        config.setProperty("td.connect.type", "ws");
        config.setProperty("bootstrap.servers", "192.168.1.98:7041");
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

        env.execute("Flink test cdc Example");
    }

    public static String printRow(RowData rowData) {
        StringBuilder sb = new StringBuilder();
        GenericRowData row = (GenericRowData) rowData;
        sb.append("table_ts: " + row.getField(0) +
                ", table_current: " + row.getFloat(1) +
                ", table_voltage: " + row.getInt(2) +
                ", table_phase: " + row.getFloat(3) +
                ", table_tbname: " + new String(row.getBinary(4)));
        sb.append("\n");
        System.out.println(sb);
        return sb.toString();
    }

    @Test
    void testTable() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String tdengineTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.98:7041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table-name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, tbname FROM `meters`'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // 使用 SQL 查询 TDengine 表
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, tbname FROM `meters` where `current` > 40"
        );

        // 将查询结果转换为 DataStream 并打印
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                boolean isRetract = value.f0;
                RowData row = value.f1;
                // 根据需要格式化 RowData 的内容
                StringBuilder sb = new StringBuilder();
                String result = printRow(row);
                sb.append(result);
                return sb.toString();
            }
        });

        // 打印结果
        formattedResult.print();
        // 启动 Flink 作业
        env.execute("Flink Table API & SQL TDengine Example");
    }
    public static String printCdcRow(RowData rowData) {
        StringBuilder sb = new StringBuilder();
        GenericRowData row = (GenericRowData) rowData;
        sb.append("table_cdc_ts: " + row.getField(0) +
                ", table_cdc_current: " + row.getFloat(1) +
                ", table_cdc_voltage: " + row.getInt(2) +
                ", table_cdc_phase: " + row.getFloat(3) +
                ", table_cdc_location: " + new String(row.getBinary(4)) +
                ", table_cdc_groupid: " + row.getInt(5));
        sb.append("\n");
        System.out.println(sb);
        return sb.toString();
    }
    @Test
    void testTableCdc() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String tdengineTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'bootstrap.servers' = '192.168.1.98:7041'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'group.id' = 'group_10'," +
                "  'auto.offset.reset' = 'earliest'," +
                "  'topic' = 'topic_meters'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // 使用 SQL 查询 TDengine 表
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, location, groupid FROM `meters` where `current` > 40"
        );

        // 将查询结果转换为 DataStream 并打印
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                boolean isRetract = value.f0;
                RowData row = value.f1;
                // 根据需要格式化 RowData 的内容
                StringBuilder sb = new StringBuilder();
                String result = printCdcRow(row);
                sb.append(result);
                return sb.toString();
            }
        });

        // 打印结果
        formattedResult.print();
        // 启动 Flink 作业
        env.execute("Flink Table API & SQL TDengine Example");
    }
    @Test
    void testTableToSink() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.98:7041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table-name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`'" +
                ")";


        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.98:7041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");
        tableResult.await();
//        // 启动 Flink 作业
//        env.execute("Flink Table API & SQL TDengine Example");
    }
}
