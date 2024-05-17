package com.taosdata.flink.sink;

import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class FlinkSinkTest {
    private static final String host = "192.168.1.95";
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
        }
    }


    @Test
    public void testFlinkSink() throws Exception {
        StatementData statementData = new StatementData();
        statementData.setDbName("power");
        statementData.setSupperTableName("meters");
        statementData.setTableName("d001");
        statementData.setTagFieldNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        statementData.setTagParams(new ArrayList<>(Arrays.asList(new Param(DataType.TSDB_DATA_TYPE_INT, 1),
                new Param(DataType.TSDB_DATA_TYPE_VARCHAR, "California.SanFrancisco"))));
        statementData.setFieldNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        statementData.setMode(1);
        statementData.setColumParams(new ArrayList<>(Arrays.asList(new StatementParam(DataType.TSDB_DATA_TYPE_TIMESTAMP,
                new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new StatementParam(DataType.TSDB_DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new StatementParam(DataType.TSDB_DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new StatementParam(DataType.TSDB_DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f))))));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StatementData> dataStream = env.fromElements(statementData);
        TaosOptions taosOptions = new TaosOptions("root", "taosdata", "192.168.1.95", 6041);
        dataStream.addSink(new TaosSinkFunction<StatementData>(taosOptions));
        env.execute("Dynamic Sink Function");

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count(*) from power.meters");
            resultSet.next();
            Assert.assertEquals(3, resultSet.getInt(1));
        }
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
