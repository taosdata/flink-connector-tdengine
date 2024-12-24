package com.taosdata.flink.source;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.common.TDengineCdcParams;
import com.taosdata.flink.sink.TDengineSink;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;

import java.io.IOException;
import java.util.List;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;


public class TDFlinkSourceTest {
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;
    String jdbcUrl = "jdbc:TAOS-WS://192.168.1.95:6041?user=root&password=taosdata";
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
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, groupid, location from meters");
        sourceQuery(sql, 3, connProps);
    }

    @Test
    void testTDengineSourceByTimeSplit() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //按照时间分片
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofHours(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));

        sourceQuery(splitSql, 3, connProps);
    }

    @Test
    void testTDengineSourceByTagSplit() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, current, voltage, phase, groupid, location from meters")
                .setTagList(Arrays.asList(
                        "groupid = 1 and location = 'California.SanFrancisco'",
                        "groupid = 2 and location = 'Alabama.Montgomery'"))
                .setSplitType(SplitType.SPLIT_TYPE_TAG);
        sourceQuery(splitSql, 3, connProps);
    }

    @Test
    void testTDengineSourceByTableSplit() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSelect("ts, current, voltage, phase, groupid, location")
                .setTableList(Arrays.asList("d1001", "d1002"))
                .setOther("order by ts limit 100")
                .setSplitType(SplitType.SPLIT_TYPE_TABLE);
        sourceQuery(splitSql, 3, connProps);
    }

    public void sourceQuery(SourceSplitSql sql, int parallelism, Properties connProps) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            GenericRowData row = (GenericRowData) rowData;
            sb.append("ts: " + row.getTimestamp(0, 0) +
                    ", current: " + row.getFloat(1) +
                    ", voltage: " + row.getInt(2) +
                    ", phase: " + row.getFloat(3) +
                    ", groupid: " + row.getInt(4) +
                    ", location: " + new String(row.getBinary(5)));
            sb.append("\n");
            totalVoltage.addAndGet(row.getInt(2));
            return sb.toString();
        });
        resultStream.print();
        env.execute("flink tdengine source");

        Assert.assertEquals(1221 * 3, totalVoltage.get());
    }

    @Test
    void testBatchTDengineSource() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Class<SourceRecords<RowData>> typeClass = (Class<SourceRecords<RowData>>) (Class<?>) SourceRecords.class;
        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname from meters");
        TDengineSource<SourceRecords<RowData>> source = new TDengineSource<>(connProps, sql, typeClass);
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
        Assert.assertEquals(1221 * 3, totalVoltage.get());
    }

    @Test
    void testTDengineSourceToSink() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.BATCH_SIZE, "1");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //按照时间分片
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofHours(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));

        tdSourceToTdSink(splitSql, 1, connProps, Arrays.asList("ts", "current", "voltage", "phase", "groupid", "location", "tbname"));
    }

    void tdSourceToTdSink(SourceSplitSql sql, int parallelism, Properties connProps, List<String> fieldNames) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(500, AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");

        DataStream<RowData> processedStream = input.flatMap(new FlatMapFunction<RowData, RowData>() {
            @Override
            public void flatMap(RowData value, Collector<RowData> out) throws Exception {
                totalVoltage.addAndGet(1);
                if (totalVoltage.get() == 10) {
                    Thread.sleep(2000L);
                    throw new IOException("custom error flag, restart application");
                }
                out.collect(value);
                Thread.sleep(100L);
            }
        });



        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");;
        sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, fieldNames);
        processedStream.sinkTo(sink);
        env.execute("flink tdengine source");
        int queryResult = queryResult();
        Assert.assertEquals(1221 * 3, queryResult);
    }


    @Test
    void testTDengineCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "192.168.1.95:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
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
        Assert.assertEquals(1221 * 3, totalVoltage.get());
    }

    @Test
    void testTDengineCdcBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "192.168.1.95:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineCdcParams.TMQ_BATCH_MODE, "true");

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
        Assert.assertEquals(1221 * 3, totalVoltage.get());
    }

    @Test
    void testTDengineCdcToTdSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(500, AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/menshibin/flink/checkpoint/");

        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "192.168.1.95:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL, "1000");
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
        sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://192.168.1.95:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.BATCH_SIZE, "2000");

        TDengineSink<ConsumerRecords<RowData>> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        input.sinkTo(sink);
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(600000L);
        jobClient.cancel().get();
        int queryResult = queryResult();
        Assert.assertEquals(1221 * 3, queryResult);
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
        Assert.assertEquals(203 * 3, totalVoltage.get());
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
        Assert.assertEquals(1221 * 3, totalVoltage.get());
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
        Assert.assertEquals(1221 * 3, queryResult);
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
        Assert.assertEquals(1221 * 3, queryResult);
    }

}
