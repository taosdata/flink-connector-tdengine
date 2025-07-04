package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.common.TDengineCdcParams;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.entity.ResultBean;
import com.taosdata.flink.source.TDengineSource;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;


public class TDFlinkSinkTest {
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
                "('2024-12-19 19:12:45.642', 50.30000, 201, 0.31000, 1) " +
                "('2024-12-19 19:12:46.642', 82.60000, 202, 0.33000, 1) " +
                "('2024-12-19 19:12:47.642', 92.30000, 203, 0.31000, 1) " +
                "('2024-12-19 18:12:45.642', 50.30000, 201, 0.31000, 1) " +
                "('2024-12-19 18:12:46.642', 82.60000, 202, 0.33000, 1) " +
                "('2024-12-19 18:12:47.642', 92.30000, 203, 0.31000, 1) " +
                "('2024-12-19 17:12:45.642', 50.30000, 201, 0.31000, 1) " +
                "('2024-12-19 17:12:46.642', 82.60000, 202, 0.33000, 1) " +
                "('2024-12-19 17:12:47.642', 92.30000, 203, 0.31000, 1) " +
                "power.d1002 USING power.meters TAGS('Alabama.Montgomery', 2) " +
                "VALUES " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 0.25000, 2) " +
                "('2024-12-19 19:12:46.642', 62.60000, 205, 0.33000, 2) " +
                "('2024-12-19 19:12:47.642', 72.30000, 206, 0.31000, 2) " +
                "('2024-12-19 18:12:45.642', 50.30000, 204, 0.25000, 2) " +
                "('2024-12-19 18:12:46.642', 62.60000, 205, 0.33000, 2) " +
                "('2024-12-19 18:12:47.642', 72.30000, 206, 0.31000, 2) " +
                "('2024-12-19 17:12:45.642', 50.30000, 204, 0.25000, 2) " +
                "('2024-12-19 17:12:46.642', 62.60000, 205, 0.33000, 2) " +
                "('2024-12-19 17:12:47.642', 72.30000, 206, 0.31000, 2) ";

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
            rowsAffected = stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts timestamp, current float, voltage int, phase float, `Aateat` int) TAGS (location binary(64), groupId int);");
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
            stmt.executeUpdate("CREATE STABLE IF NOT EXISTS sink_meters (ts timestamp, current float, voltage int, phase float, `Aateat` int) TAGS (location binary(64), groupId int);");
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
    void testPrepare() {
        System.out.println("XXXXXX");
    }
    @Test
    void testTDengineSourceToSink() throws Exception {
        System.out.println("testTDengineSourceToSink start！");
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "1");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //split by time
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofHours(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));

        tdSourceToTdSink(splitSql, 1, connProps, Arrays.asList("ts", "current", "voltage", "phase", "groupid", "location", "tbname"), "");
        System.out.println("testTDengineSourceToSink finish！");
    }

    void tdSourceToTdSink(SourceSplitSql sql, int parallelism, Properties connProps, List<String> fieldNames, String normaltableName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(500, AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(4);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");

        DataStream<RowData> processedStream = input.flatMap(new FlatMapFunction<RowData, RowData>() {
            @Override
            public void flatMap(RowData value, Collector<RowData> out) throws Exception {
                int nCount = totalVoltage.addAndGet(1);
                if (nCount == 10) {
                    Thread.sleep(2000L);
                    throw new IOException("custom error flag, restart application");
                }
                out.collect(value);
                Thread.sleep(100L);
                System.out.println("to sink:" + totalVoltage.toString());
            }
        });

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
        ;
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        if (Strings.isNullOrEmpty(normaltableName)) {
            sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        } else {
            sinkProps.setProperty(TDengineConfigParams.TD_TABLE_NAME, normaltableName);
        }
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, fieldNames);
        processedStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = 0;
        if (Strings.isNullOrEmpty(normaltableName)) {
            queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
            Assert.assertEquals(1221 * 3, queryResult);
        } else {
            queryResult = queryResult("SELECT sum(voltage) FROM power_sink." + normaltableName);
            Assert.assertEquals(606 * 3, queryResult);
        }

    }

    @Test
    void testTDengineNormal() throws Exception {
        System.out.println("testTDengineNormal start！");
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "1");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, `current`, voltage, phase from d1001")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //split by time
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofHours(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));

        tdSourceToTdSink(splitSql, 3, connProps, Arrays.asList("ts", "current", "voltage", "phase"), "sink_normal");
        System.out.println("testTDengineNormal finish！");
    }

    @Test
    void testCustomTypeTDengineCdc() throws Exception {
        System.out.println("testCustomTypeTDengineCdc start！");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(4);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultDeserializer");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        TDengineCdcSource<ResultBean> tdengineSource = new TDengineCdcSource<>("topic_meters", config, ResultBean.class);
        DataStreamSource<ResultBean> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultBeanSinkSerializer");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_cdc");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<ResultBean> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        input.sinkTo(sink);
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(8000L);
        jobClient.cancel().get();
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testCustomTypeTDengineCdc finish！");
    }


    @Test
    void testTDengineCdcBatch() throws Exception {
        System.out.println("testTDengineCdcBatch start！");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineCdcParams.TMQ_BATCH_MODE, "true");

        Class<ConsumerRecords<RowData>> typeClass = (Class<ConsumerRecords<RowData>>) (Class<?>) ConsumerRecords.class;
        TDengineCdcSource<ConsumerRecords<RowData>> tdengineSource = new TDengineCdcSource<>("topic_meters", config, typeClass);
        DataStreamSource<ConsumerRecords<RowData>> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<ConsumerRecords<RowData>, String>) records -> {
            Iterator<ConsumerRecord<RowData>> iterator = records.iterator();
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()) {
                RowData row = iterator.next().value();
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
        Thread.sleep(8000L);
        jobClient.cancel().get();
        Assert.assertEquals(1221 * 3, totalVoltage.get());
        System.out.println("testTDengineCdcBatch finish！");
    }

    @Test
    void testTDengineCdcToTdSink() throws Exception {
        System.out.println("testTDengineCdcToTdSink start！");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(500, AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineCdcParams.TMQ_BATCH_MODE, "true");

        Class<ConsumerRecords<RowData>> typeClass = (Class<ConsumerRecords<RowData>>) (Class<?>) ConsumerRecords.class;
        TDengineCdcSource<ConsumerRecords<RowData>> tdengineSource = new TDengineCdcSource<>("topic_meters", config, typeClass);
        DataStreamSource<ConsumerRecords<RowData>> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_cdc");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<ConsumerRecords<RowData>> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        input.sinkTo(sink);
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(8000L);
        jobClient.cancel().get();
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testTDengineCdcToTdSink finish！");
    }

    @Test
    void testCheckpointTDengineCdc() throws Exception {
        System.out.println("testCheckpointTDengineCdc start！");
        JobClient jobClient =checkpointTDengineCdc();
        Thread.sleep(4000L);
        jobClient.cancel().get();

        jobClient =checkpointTDengineCdc();
        Thread.sleep(8000L);
        jobClient.cancel().get();

        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testCheckpointTDengineCdc finish！");
    }
    private JobClient checkpointTDengineCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500, AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(7);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");


        TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");

        DataStream<RowData> processedStream = input.flatMap(new FlatMapFunction<RowData, RowData>() {
            @Override
            public void flatMap(RowData value, Collector<RowData> out) throws Exception {
                int nCount = totalVoltage.addAndGet(1);
                if (nCount == 10) {
                    Thread.sleep(2000L);
//                    throw new IOException("custom error flag, restart application");
                }

                out.collect(value);
                System.out.println("to sink:" + totalVoltage.toString());
                Thread.sleep(100);
            }
        });

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_cdc");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        processedStream.sinkTo(sink);
        return env.executeAsync("Flink test cdc Example");
    }

    @Test
    void testBatchTDengineSource() throws Exception {
        System.out.println("testBatchTDengineSource start！");
        Properties connProps = new Properties();
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Class<SourceRecords<RowData>> typeClass = (Class<SourceRecords<RowData>>) (Class<?>) SourceRecords.class;
        SourceSplitSql sql = new SourceSplitSql("select ts, `current`, voltage, phase, location, groupid, tbname from meters");
        TDengineSource<SourceRecords<RowData>> source = new TDengineSource<>(connProps, sql, typeClass);
        DataStreamSource<SourceRecords<RowData>> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");
        TDengineSink<SourceRecords<RowData>> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        input.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(1221 * 3, queryResult);
        System.out.println("testBatchTDengineSource finish！");
    }

    @Test
    void  testSinkFromJsonData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // try 3
                Time.of(10, TimeUnit.SECONDS) // wait for 10 seconds after each failure
        ));
        String[] jsonArray = new String[20];
        for (int i = 0; i < 20; i++) {
            jsonArray[i] = "{\"vin\":" + i % 10 +"}";
        }
        DataStream<String> dataStream = env.fromElements(String.class, jsonArray);
        SingleOutputStreamOperator<RowData> messageStream = dataStream.map(new RichMapFunction<String, RowData>() {
            @Override
            public RowData map(String value) throws Exception {
                Random random = new Random(System.currentTimeMillis());
                GenericRowData rowData = new GenericRowData(7);
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(value);
                int vin = jsonNode.get("vin").asInt();
                int nCount = totalVoltage.addAndGet(1);
                long current = System.currentTimeMillis() + nCount * 1000;
                rowData.setField(0, TimestampData.fromEpochMillis(current)); // ts
                rowData.setField(1, random.nextFloat() * 30); // current
                rowData.setField(2, 300 + vin); // voltage
                rowData.setField(3, random.nextFloat()); // phase
                rowData.setField(4, StringData.fromString("location_" + vin)); // location
                rowData.setField(5, vin); // groupid
                rowData.setField(6, StringData.fromString("d0" + vin)); // tbname
                return rowData;
            }
        });
        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        messageStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(6090, queryResult);
        System.out.println("testBatchTDengineSource finish！");

    }

    @Test
    void  testSinkOfRowData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RowData[] rowDatas = new GenericRowData[10];
//        "ts", "current", "voltage", "phase", "location", "groupid", "tbname"
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(8);
            long current = System.currentTimeMillis() + i * 1000;
            row.setField(0, TimestampData.fromEpochMillis(current));
            row.setField(1, random.nextFloat() * 30);
            row.setField(2, 300 + (i + 1));
            row.setField(3, random.nextFloat());
            row.setField(4, random.nextInt());
            row.setField(5, StringData.fromString("location_" + i));
            row.setField(6, i);
            row.setField(7, StringData.fromString("d0" + i));
            rowDatas[i] = row;
        }

        DataStream<RowData> dataStream = env.fromElements(RowData.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "`Aateat`","location", "groupid", "tbname"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(3055, queryResult);
        System.out.println("testBatchTDengineSource finish！");
    }

    @Test
    void  testNormalTableSinkOfRowData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RowData[] rowDatas = new GenericRowData[10];
//        "ts", "current", "voltage", "phase", "location", "groupid", "tbname"
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(4);
            long current = System.currentTimeMillis() + i * 1000;
            row.setField(0, TimestampData.fromEpochMillis(current));
            row.setField(1, random.nextFloat() * 30);
            row.setField(2, 300 + (i + 1));
            row.setField(3, random.nextFloat());
            rowDatas[i] = row;
        }

        DataStream<RowData> dataStream = env.fromElements(RowData.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_TABLE_NAME, "sink_normal");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_normal");
        Assert.assertEquals(3055, queryResult);
        System.out.println("testBatchTDengineSource finish！");
    }

    @Test
    void  testSinkOfCustomType() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ResultBean[] rowDatas = new ResultBean[10];
//        "ts", "current", "voltage", "phase", "location", "groupid", "tbname"
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            ResultBean rowData = new ResultBean();
            long current = System.currentTimeMillis() + i * 1000;
            rowData.setTs(new Timestamp(current));
            rowData.setCurrent(random.nextFloat() * 30);
            rowData.setVoltage(300 + (i + 1));
            rowData.setPhase(random.nextFloat());
            rowData.setLocation("location_" + i);
            rowData.setGroupid(i);
            rowData.setTbname("d0" + i);
            rowDatas[i] = rowData;
        }

        DataStream<ResultBean> dataStream = env.fromElements(ResultBean.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultBeanSinkSerializer");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<ResultBean> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult("SELECT sum(voltage) FROM power_sink.sink_meters");
        Assert.assertEquals(3055, queryResult);
        System.out.println("testBatchTDengineSource finish！");
    }
}
