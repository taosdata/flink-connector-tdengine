package com.taosdata.flink.sink;

import com.taosdata.flink.sink.entity.*;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkSinkTest {
    private static final String host = "127.0.0.1";
    private Connection connection;

    @Before
    public void before() throws SQLException {

        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");

        this.connection = DriverManager.getConnection(url, properties);
        try (Statement statement = this.connection.createStatement()) {
            statement.executeUpdate("drop database if exists power");

            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
            // use database
            statement.executeUpdate("USE power");
            // create table
            statement.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)");
            for (int i = 3; i <= 5; i++ ) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS d00"+ i + " USING meters (groupId, location) TAGS (" + i + ", 'California.SanFrancisco')");
            }
        }
    }

    @Test
    public void testFlinkSink() throws Exception {
        SuperTableData superTableData = new SuperTableData("power");
        superTableData.setSuperTableName("meters");
        superTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        superTableData.setColumnNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 1; i <= 2; i++ ) {
            SubTableData subTableData = new SubTableData();
            subTableData.setTableName("d00" + i);

            subTableData.setTagParams(new ArrayList<>(Arrays.asList( new TagParam(DataType.DATA_TYPE_INT, i), new TagParam(DataType.DATA_TYPE_VARCHAR, "California.SanFrancisco"))));
            subTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                    new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
            )));
            subTableDataList.add(subTableData);
        }
        superTableData.setSubTableDataList(subTableDataList);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SuperTableData> dataStream = env.fromElements(superTableData);
        String url  = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<SuperTableData>(url, connProps));
        env.execute("Dynamic Sink Function");
    }

    @Test
    public void testNotTagFlinkSink() throws Exception {
        SuperTableData superTableData = new SuperTableData("power");
        superTableData.setColumnNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        superTableData.setSuperTableName("meters");
        superTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 3; i <= 5; i++ ) {
            SubTableData subTableData = new SubTableData();
            subTableData.setTableName("d00" + i);
            subTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                    new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
            )));
            subTableDataList.add(subTableData);
        }
        superTableData.setSubTableDataList(subTableDataList);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SuperTableData> dataStream = env.fromElements(superTableData);
        String url  = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<SuperTableData>(url, connProps));
        env.execute("Dynamic Sink Function");
    }

    @Test
    public void testSqlFlinkSink() throws Exception {
        SuperTableData superTableData = new SuperTableData("power");
        superTableData.setSuperTableName("meters");
        superTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        superTableData.setColumnNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 1; i <= 2; i++ ) {
            SubTableData subTableData = new SubTableData();
            subTableData.setTableName("d00" + i);
            subTableData.setTagParams(new ArrayList<>(Arrays.asList( new TagParam(DataType.DATA_TYPE_INT, i), new TagParam(DataType.DATA_TYPE_VARCHAR, "California.SanFrancisco"))));
            subTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                    new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
            )));
            subTableDataList.add(subTableData);
        }
        superTableData.setSubTableDataList(subTableDataList);
        SqlData sqlData = new SqlData("", getStringList());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, superTableData, sqlData);


        String url  = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<>(url, connProps));
        env.execute("Dynamic Sink Function");
    }

    @Test
    public void testSqlErrorIgnore() throws Exception {
        String dbname = "case11db";
        List<String> sqlList = new ArrayList<>();
        sqlList.add("drop database if exists " + dbname);
        sqlList.add("CREATE DATABASE IF NOT EXISTS " + dbname);
        sqlList.add("use " + dbname);
        sqlList.add("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
// 这里 sql 是错误的
        sqlList.add("CREATE STABLE IF NOT EXISTS meters2 (ts TIMESTAMPss, ts2 TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        sqlList.add("drop table meters2");
        sqlList.add("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)");
        sqlList.add("alter table test add column c1 TIMESTAMP");
        SqlData sqlData = new SqlData("", sqlList);

        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connProps.put(TSDBDriver.PROPERTY_KEY_BATCH_ERROR_IGNORE, "true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, sqlData);
        String url  = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        dataStream.addSink(new TaosSinkConnector<>(url, connProps));
        env.execute("Dynamic Sink Function");
    }
    @Test
    public void testSqlErrorStop() throws Exception {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("drop database if exists case3db");
        sqlList.add("CREATE DATABASE IF NOT EXISTS case3db");
        sqlList.add("use case3db");
        sqlList.add("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        sqlList.add("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)");
        SqlData sqlData = new SqlData("", sqlList);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, sqlData);
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<>(url, connProps));
        env.execute("Dynamic Sink Function");
    }
    @Test
    public void testNormalTableFlinkSink() throws Exception {
        SuperTableData superTableData = new SuperTableData("power");
        superTableData.setSuperTableName("meters");
        superTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        superTableData.setColumnNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 1; i <= 2; i++ ) {
            SubTableData subTableData = new SubTableData();
            subTableData.setTableName("d00" + i);
            subTableData.setTagParams(new ArrayList<>(Arrays.asList( new TagParam(DataType.DATA_TYPE_INT, i), new TagParam(DataType.DATA_TYPE_VARCHAR, "California.SanFrancisco"))));
            subTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                    new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                    new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
            )));
            subTableDataList.add(subTableData);
        }
        superTableData.setSubTableDataList(subTableDataList);
        SqlData sqlData = new SqlData("", getStringList());

        NormalTableData normalTableData = new NormalTableData("power", "test");
        normalTableData.setColumnNames(superTableData.getColumnNames());
        normalTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
        )));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, superTableData, sqlData, normalTableData);
        String url  = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<>(url, connProps));
        env.execute("Dynamic Sink Function");

    }
    @NotNull
    private static List<String> getStringList() {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String insertQuery = "INSERT INTO " +
                    "power.d100" + i + " USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 219, 0.31000) " +
                    "(NOW + 2a, 12.60000, 218, 0.33000) " +
                    "(NOW + 3a, 12.30000, 221, 0.31000) " +
                    "power.d100" + (i+1) +" USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 218, 0.25000) ";

            sqls.add(insertQuery);
        }

        return sqls;
    }
    @After
    public void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop database if exists power");
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }

}
