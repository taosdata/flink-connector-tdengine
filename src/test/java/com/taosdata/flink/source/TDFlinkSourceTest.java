package com.taosdata.flink.source;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.common.TDengineConnectorParams;
import com.taosdata.flink.common.TDengineTmqParams;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;


public class TDFlinkSourceTest {
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;
    String jdbcUrl = "jdbc:TAOS-WS://192.168.1.95:6041?user=root&password=taosdata";
    static AtomicInteger totalVoltage = new AtomicInteger();
    public void prepare() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String insertQuery = "INSERT INTO " +
                "power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) " +
                "VALUES " +
                "(NOW + 1a, 50.30000, 201, 0.31000) " +
                "(NOW + 2a, 82.60000, 202, 0.33000) " +
                "(NOW + 3a, 92.30000, 203, 0.31000) " +
                "power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) " +
                "VALUES " +
                "(NOW + 1a, 50.30000, 204, 0.25000) " +
                "(NOW + 2a, 62.60000, 205, 0.33000) " +
                "(NOW + 3a, 72.30000, 206, 0.31000) ";
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement()) {

            stmt.executeUpdate("DROP TOPIC IF EXISTS topic_meters");

            stmt.executeUpdate("DROP database IF EXISTS power");
            // create database
            int rowsAffected = stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power vgroups 5");

            stmt.executeUpdate("use power");
            // you can check rowsAffected here
            System.out.println("Create database power successfully, rowsAffected: " + rowsAffected);
            // create table
            rowsAffected = stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            // you can check rowsAffected here
            System.out.println("Create stable power.meters successfully, rowsAffected: " + rowsAffected);

            stmt.executeUpdate("CREATE TOPIC topic_meters as SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM meters");

            int affectedRows = stmt.executeUpdate(insertQuery);
            // you can check affectedRows here
            System.out.println("Successfully inserted " + affectedRows + " rows to power.meters.");

            stmt.executeUpdate("DROP database IF EXISTS power_sink");
            // create database
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power_sink vgroups 5");

            stmt.executeUpdate("use power_sink");
            // you can check rowsAffected here
            System.out.println("Create database power successfully, rowsAffected: " + rowsAffected);
            // create table
            stmt.executeUpdate("CREATE STABLE IF NOT EXISTS sink_meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            // you can check rowsAffected here


        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            throw ex;
        }
    }

    public int queryResult() throws Exception {
        String sql = "SELECT sum(voltage) FROM power_sink.sink_meters";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement();
             // query data, make sure the database and table are created before
             ResultSet resultSet = stmt.executeQuery(sql)) {

            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
            return -1;
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to query data from power.meters, sql: %s, %sErrMessage: %s%n",
                    sql,
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }


    @BeforeEach
    void beforeEach() throws Exception {
        totalVoltage.set(0);
        prepare();
        reporter = InMemoryReporter.create();
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(5)
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
        connProps.setProperty(TDengineConnectorParams.VALUE_DESERIALIZER, "RowData");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TDengineSource<RowData> source = new TDengineSource<>("jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata", connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("ts: " + row.getTimestamp(0, 0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", location: " + new String(row.getBinary(4)));
            sb.append("\n");
            totalVoltage.addAndGet(row.getInt(2));
            return sb.toString();
        });
        resultStream.print();
        env.execute("flink tdengine source");

        Assert.assertEquals(1221, totalVoltage.get());
    }

    @Test
    void testBatchTDengineSource() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConnectorParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConnectorParams.TD_BATCH_MODE, "true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Class<SourceRecords<RowData>> typeClass = (Class<SourceRecords<RowData>>) (Class<?>) SourceRecords.class;
        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TDengineSource<SourceRecords<RowData>> source = new TDengineSource<>("jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata", connProps, sql, typeClass);
        DataStreamSource<SourceRecords<RowData>> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<SourceRecords<RowData>, String>) records -> {
            StringBuilder sb = new StringBuilder();
            Iterator<RowData> iterator = records.iterator();
            while (iterator.hasNext()) {
                GenericRowData row = (GenericRowData) iterator.next();
                sb.append("ts: " + row.getTimestamp(0, 0) +
                        ", current: " + row.getFloat(1) +
                        ", voltage: " + row.getInt(2) +
                        ", phase: " + row.getFloat(3) +
                        ", location: " + new String(row.getBinary(4)));
                sb.append("\n");
                totalVoltage.addAndGet(row.getInt(2));
            }
            return sb.toString();
        });
        resultStream.print();
        env.execute("flink tdengine source");
        Assert.assertEquals(1221, totalVoltage.get());
    }

    @Test
    void testTDengineCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties config = new Properties();
        config.setProperty(TDengineTmqParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineTmqParams.BOOTSTRAP_SERVERS, "192.168.1.95:6041");
        config.setProperty(TDengineTmqParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineTmqParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineTmqParams.AUTO_COMMIT_INTERVAL, "1000");
        config.setProperty(TDengineTmqParams.GROUP_ID, "group_1");
        config.setProperty(TDengineTmqParams.CONNECT_USER, "root");
        config.setProperty(TDengineTmqParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineTmqParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineTmqParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("tsxx: " + row.getTimestamp(0, 0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", location: " + new String(row.getBinary(4)));
            sb.append("\n");
            totalVoltage.addAndGet(row.getInt(2));
            return sb.toString();
        });
        resultStream.print();
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(5000L);
        jobClient.cancel().get();
        Assert.assertEquals(1221, totalVoltage.get());
    }

    @Test
    void testTDengineCdcBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties config = new Properties();
        config.setProperty(TDengineTmqParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineTmqParams.BOOTSTRAP_SERVERS, "192.168.1.95:6041");
        config.setProperty(TDengineTmqParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineTmqParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineTmqParams.AUTO_COMMIT_INTERVAL, "1000");
        config.setProperty(TDengineTmqParams.GROUP_ID, "group_1");
        config.setProperty(TDengineTmqParams.CONNECT_USER, "root");
        config.setProperty(TDengineTmqParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineTmqParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineTmqParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineTmqParams.TMQ_BATCH_MODE, "true");

        Class<ConsumerRecords<RowData>> typeClass = (Class<ConsumerRecords<RowData>>) (Class<?>) ConsumerRecords.class;
        TDengineCdcSource<ConsumerRecords<RowData>> tdengineSource = new TDengineCdcSource<>("topic_meters", config, typeClass);
        DataStreamSource<ConsumerRecords<RowData>> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<ConsumerRecords<RowData>, String>) records -> {
            Iterator<ConsumerRecord<RowData>> iterator = records.iterator();
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()) {
                GenericRowData row = (GenericRowData) iterator.next().value();
                sb.append("tsxx: " + row.getTimestamp(0, 0) +
                        ", current: " + row.getFloat(1) +
                        ", voltage: " + row.getInt(2) +
                        ", phase: " + row.getFloat(3) +
                        ", location: " + new String(row.getBinary(4)));
                sb.append("\n");
                totalVoltage.addAndGet(row.getInt(2));
            }
            return sb.toString();

        });

        resultStream.print();
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(5000L);
        jobClient.cancel().get();
        Assert.assertEquals(1221, totalVoltage.get());
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
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table-name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, tbname FROM `meters`'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // 使用 SQL 查询 TDengine 表
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, tbname FROM `meters` where `current` > 90"
        );

        // 将查询结果转换为 DataStream 并打印
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                RowData rowData = value.f1;
                // 根据需要格式化 RowData 的内容
                StringBuilder sb = new StringBuilder();
                GenericRowData row = (GenericRowData) rowData;
                sb.append("table_ts: " + row.getTimestamp(0, 0) +
                        ", table_current: " + row.getFloat(1) +
                        ", table_voltage: " + row.getInt(2) +
                        ", table_phase: " + row.getFloat(3) +
                        ", table_tbname: " + new String(row.getBinary(4)));
                totalVoltage.addAndGet(row.getInt(2));
                sb.append("\n");
                return sb.toString();
            }
        });

        // 打印结果
        formattedResult.print();
        // 启动 Flink 作业
        env.execute("Flink Table API & SQL TDengine Example");
        Assert.assertEquals(203, totalVoltage.get());

    }

    @Test
    void testTableCdc() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARBINARY," +
                " groupid INT," +
                " tbname VARBINARY" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'bootstrap.servers' = '192.168.1.95:6041'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'group.id' = 'group_20'," +
                "  'auto.offset.reset' = 'earliest'," +
                "  'enable.auto.commit' = 'false'," +
                "  'topic' = 'topic_meters'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // 使用 SQL 查询 TDengine 表
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters` where `current` > 40"
        );

        // 将查询结果转换为 DataStream 并打印
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                RowData rowData = value.f1;
                StringBuilder sb = new StringBuilder();
                GenericRowData row = (GenericRowData) rowData;
                sb.append("table_cdc_ts: " + row.getTimestamp(0, 0) +
                        ", table_cdc_current: " + row.getFloat(1) +
                        ", table_cdc_voltage: " + row.getInt(2) +
                        ", table_cdc_phase: " + row.getFloat(3) +
                        ", table_cdc_location: " + new String(row.getBinary(4)) +
                        ", table_cdc_groupid: " + row.getInt(5) +
                        ", table_cdc_tbname: " + new String(row.getBinary(4)));
                sb.append("\n");
                totalVoltage.addAndGet(row.getInt(2));
                return sb.toString();
            }
        });

        // 打印结果
        formattedResult.print();
        // 启动 Flink 作业
        JobClient jobClient = env.executeAsync("Flink Table API & SQL TDengine Example");
        Thread.sleep(5000L);
        jobClient.cancel().get();
        Assert.assertEquals(1221, totalVoltage.get());
    }

    @Test
    void testTableToSink() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
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
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata'," +
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
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.95:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");
        tableResult.await();
        int queryResult = queryResult();
        Assert.assertEquals(1221, queryResult);
    }

    @Test
    void testCdcTableToSink() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.enableCheckpointing(1000, AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
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
                "  'bootstrap.servers' = '192.168.1.95:6041'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'group.id' = 'group_22'," +
                "  'auto.offset.reset' = 'earliest'," +
                "  'enable.auto.commit' = 'false'," +
                "  'topic' = 'topic_meters'" +
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
                "  'td.jdbc.mode' = 'cdc'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://192.168.1.95:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");

        Thread.sleep(5000L);
        tableResult.getJobClient().get().cancel().get();
        int queryResult = queryResult();
        Assert.assertEquals(1221, queryResult);
    }

}
