package com.taosdata.flink.table;

import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;

public class TDFlinkTableTest {
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;
    String jdbcUrl = "jdbc:TAOS-WS://localhost:6041?user=root&password=taosdata";
    static AtomicInteger totalVoltage = new AtomicInteger();
    LocalDateTime insertTime;

    public void prepare() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String insertQuery = "INSERT INTO " +
                "power.d1001 USING power.meters TAGS('California.SanFrancisco', 1) " +
                "VALUES " +
                "('2024-12-19 19:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 19:12:45.642', 50.30000, 201, 1.31001) " +
                "('2024-12-19 19:12:45.642', 50.30000, 201, 2.31002) " +
                "('2024-12-19 19:12:45.642', 50.30000, 201, 3.31003) " +
                "('2024-12-19 19:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 19:12:47.642', 92.30000, 203, 0.31000) " +
                "('2024-12-19 18:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 18:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 18:12:47.642', 92.30000, 203, 0.31000) " +
                "('2024-12-19 17:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 17:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 17:12:47.642', 92.30000, 203, 0.31000) " +
                "power.d1002 USING power.meters TAGS('Alabama.Montgomery', 2) " +
                "VALUES " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 1.25001) " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 2.25002) " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 3.25003) " +
                "('2024-12-19 19:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 19:12:47.642', 72.30000, 206, 0.31000) " +
                "('2024-12-19 18:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 18:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 18:12:47.642', 72.30000, 206, 0.31000) " +
                "('2024-12-19 17:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 17:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 17:12:47.642', 72.30000, 206, 0.31000) ";

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
            insertTime = LocalDateTime.now();
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

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS sink_normal (ts timestamp, current float, voltage int, phase float);");
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

    public int queryResult(String sql) throws Exception {
        if (Strings.isEmpty(sql)) {
            sql = "SELECT sum(voltage) FROM power_sink.sink_meters";
        }
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
    void testTable() throws Exception {
        System.out.println("testTable start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String tdengineTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table.name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, tbname FROM `meters`'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // using Fink SQL to query meters table
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, tbname FROM `meters` where `current` > 90"
        );

        // convert query results to DataStream and print
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                RowData rowData = value.f1;
                // format the contents of RowData as needed
                StringBuilder sb = new StringBuilder();
                RowData row = rowData;
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

        // print result
        formattedResult.print();
        // start Flink task
        env.execute("Flink Table API & SQL TDengine Example");
        Assert.assertEquals(203 * 3, totalVoltage.get());
        System.out.println("testTable finish！");
    }

    @Test
    void testTableCdc() throws Exception {
        System.out.println("testTableCdc start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'bootstrap.servers' = 'localhost:6041'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'group.id' = 'group_20'," +
                "  'auto.offset.reset' = 'earliest'," +
                "  'enable.auto.commit' = 'false'," +
                "  'topic' = 'topic_meters'" +
                ")";

        tableEnv.executeSql(tdengineTableDDL);
        // using Fink SQL to query meters table
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters` where `current` > 40"
        );

        // convert query results to DataStream and print
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                RowData rowData = value.f1;
                StringBuilder sb = new StringBuilder();
                RowData row = rowData;
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

        // print result
        formattedResult.print();
        // start Flink task
        JobClient jobClient = env.executeAsync("Flink Table API & SQL TDengine Example");
        Thread.sleep(8000L);
        jobClient.cancel().get();
        Assert.assertEquals(1221 * 3, totalVoltage.get());
        System.out.println("testTableCdc finish！");
    }

    @Test
    void testTableToSink() throws Exception {
        System.out.println("testTableToSink start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)," +
                " PRIMARY KEY (ts) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table.name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`'" +
                ")";


        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");
        tableResult.await();
        int queryResult = queryResult("");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testTableToSink finish！");
    }

    @Test
    void testCdcTableToSink() throws Exception {
        System.out.println("testCdcTableToSink start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.enableCheckpointing(1000, AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'bootstrap.servers' = 'localhost:6041'," +
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
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");

        Thread.sleep(8000L);
        tableResult.getJobClient().get().cancel().get();
        int queryResult = queryResult("");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testCdcTableToSink finish！");
    }


    @Test
    void testSql() throws Exception {
        System.out.println("testSql start！");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // Create Flink target table
        String tdengineSinkTableDDL = "CREATE TABLE `sink_normal` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.table.name' = 'sink_normal'" +
                ")";
        tEnv.executeSql(tdengineSinkTableDDL);
        TableResult tableResult = tEnv.executeSql("INSERT INTO sink_normal VALUES (CAST('2025-01-19 23:00:03' AS TIMESTAMP(6)), 12.34, 220, 1.56)");
        tableResult.await();
        int queryResult = queryResult("select sum(voltage) from power_sink.sink_normal");
        Assert.assertEquals(220, queryResult);
        System.out.println("testSql finish！");
    }

    @Test
    void testSuperTableSink() throws Exception {
        System.out.println("testSuperTableSink start！");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // Create Flink target table
        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";
        tEnv.executeSql(tdengineSinkTableDDL);

        String insertQuery = "INSERT INTO sink_meters " +
                "VALUES " +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 201, 3.31003, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 82.60000, 202, 0.33000, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 92.30000, 203, 0.31000, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 204, 3.25003, 'Alabama.Montgomery', 2, 'd1002')," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 62.60000, 205, 0.33000, 'Alabama.Montgomery', 2, 'd1002')," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 72.30000, 206, 0.31000, 'Alabama.Montgomery', 2, 'd1002');";

        TableResult tableResult = tEnv.executeSql(insertQuery);
        tableResult.await();
        int queryResult = queryResult("");
        Assert.assertEquals(1221, queryResult);
        System.out.println("testSuperTableSink finish！");
    }

    @Test
    void testTableSinkOfRow() throws Exception {
        System.out.println("testTableSinkOfRow start！");
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode() // use batch processing mode
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // create Flink target table
        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";
        tEnv.executeSql(tdengineSinkTableDDL);

        List<Row> rowDatas = new ArrayList<>();
//        "ts", "current", "voltage", "phase", "location", "groupid", "tbname"
        Random random = new Random(System.currentTimeMillis());
        int sum = 0;
        for (int i = 0; i < 50; i++) {
            String tbname = "d001";
            String location = "California.SanFrancisco";
            int groupId = 1;

//            if (i >= 25) {
//                tbname = "d002";
//                location = "Alabama.Montgomery";
//                groupId = 2;
//            }
            sum += 300 + (i + 1);
            long timestampInMillis = System.currentTimeMillis() + i * 1000;
            Row row = Row.of(
                    new Timestamp(timestampInMillis), // 时间递增
                    random.nextFloat() * 30,
                    300 + (i + 1),
                    random.nextFloat(),
                    location,
                    groupId,
                    tbname

            );
            rowDatas.add(row);
        }

        Table inputTable = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD("current", DataTypes.FLOAT()),
                        DataTypes.FIELD("voltage", DataTypes.INT()),
                        DataTypes.FIELD("phase", DataTypes.FLOAT()),
                        DataTypes.FIELD("location", DataTypes.STRING()),
                        DataTypes.FIELD("groupid", DataTypes.INT()),
                        DataTypes.FIELD("tbname", DataTypes.STRING())
                ),
                rowDatas
        );

        // perform batch insertion
        TableResult result = inputTable.executeInsert("sink_meters");
        result.await(); // waiting for homework to complete
        int queryResult = queryResult("");
        Assert.assertEquals(sum, queryResult);
        System.out.println("testTableSinkOfRow finish！");

    }

}
