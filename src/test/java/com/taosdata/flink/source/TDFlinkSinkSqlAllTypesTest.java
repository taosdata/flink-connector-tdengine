package com.taosdata.flink.source;

import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.sink.TDengineSink;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TDFlinkSinkSqlAllTypesTest {
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;
    String jdbcUrl = "jdbc:TAOS-WS://localhost:6041?user=root&password=taosdata";
    static AtomicInteger totalVoltage = new AtomicInteger();
    LocalDateTime insertTime;

    private static final String host = "localhost";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int BINARY_COLUMN_SIZE = 30;
    private List<String> schemaList;

    private static final int numOfSubTable = 10, numOfRow = 10;

    public void prepare() throws Exception {

        String jdbcUrl = "jdbc:TAOS-WS://" + host + ":6041/";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {

            init(conn);

            stmtAll(conn);

        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed
            // exceptions info
            System.out.println("Failed to insert data using stmt, ErrCode:" + ex.getErrorCode() + "; ErrMessage: "
                    + ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            System.out.println("Failed to insert data using stmt, ErrMessage: " + ex.getMessage());
            throw ex;
        }
    }

    private void init(Connection conn) throws SQLException, InterruptedException {
        schemaList = new ArrayList<>();
        schemaList.add("drop database if exists example_all_type_stmt0");
        schemaList.add("drop database if exists example_all_type_stmt1");
        schemaList.add("CREATE DATABASE IF NOT EXISTS example_all_type_stmt0");
        schemaList.add("CREATE DATABASE IF NOT EXISTS example_all_type_stmt1");

        schemaList.add("USE example_all_type_stmt1");
        String table = "CREATE TABLE IF NOT EXISTS normal_test" +
                "(ts TIMESTAMP, " +
                "int_col INT, " +
                "long_col BIGINT, " +
                "double_col DOUBLE, " +
                "bool_col BOOL, " +
                "binary_col BINARY(100), " +
                "nchar_col NCHAR(100))";
        schemaList.add(table);

        for (int i = 1; i >= 0; i--) {
            schemaList.add("USE example_all_type_stmt" + i);
            table = "CREATE STABLE IF NOT EXISTS stb" + i +
                    "(ts TIMESTAMP, " +
                    "int_col INT, " +
                    "long_col BIGINT, " +
                    "double_col DOUBLE, " +
                    "bool_col BOOL, " +
                    "binary_col BINARY(100), " +
                    "nchar_col NCHAR(100))" +
                    "tags (" +
                    "int_tag INT, " +
                    "long_tag BIGINT, " +
                    "double_tag DOUBLE, " +
                    "bool_tag BOOL, " +
                    "binary_tag BINARY(100), " +
                    "nchar_tag NCHAR(100))";
            schemaList.add(table);
        }


        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < schemaList.size(); i++) {
                stmt.execute(schemaList.get(i));
            }
        }
        Thread.sleep(1000);
    }

    private void stmtAll(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb0 tags(?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            // set table name
            pstmt.setTableName("ntb");
            // set tags
            pstmt.setTagInt(1, 1);
            pstmt.setTagLong(2, 1000000000000L);
            pstmt.setTagDouble(3, 1.1);
            pstmt.setTagBoolean(4, true);
            pstmt.setTagString(5, "binary_value");
            pstmt.setTagNString(6, "nchar_value");

            long current = System.currentTimeMillis();

            pstmt.setTimestamp(1, new Timestamp(current));
            pstmt.setInt(2, 1);
            pstmt.setLong(3, 1000000000000L);
            pstmt.setDouble(4, 1.1);
            pstmt.setBoolean(5, true);
            pstmt.setString(6, "binary_value");
            pstmt.setNString(7, "nchar_value");

            pstmt.addBatch();
            pstmt.executeBatch();

            pstmt.setTableName("w3");
            pstmt.setTagNull(1, TSDBConstants.TSDB_DATA_TYPE_INT);
            pstmt.setTagNull(2, TSDBConstants.TSDB_DATA_TYPE_BIGINT);
            pstmt.setTagNull(3, TSDBConstants.TSDB_DATA_TYPE_DOUBLE);
            pstmt.setTagNull(4, TSDBConstants.TSDB_DATA_TYPE_BOOL);
            pstmt.setTagNull(5, TSDBConstants.TSDB_DATA_TYPE_BINARY);
            pstmt.setTagNull(6, TSDBConstants.TSDB_DATA_TYPE_NCHAR);

            pstmt.setTimestamp(1, new Timestamp(current + 1));
            pstmt.setNull(2, Types.INTEGER);
            pstmt.setNull(3, Types.BIGINT);
            pstmt.setNull(4, Types.DOUBLE);
            pstmt.setNull(5, Types.BOOLEAN);
            pstmt.setNull(6, Types.BINARY);
            pstmt.setNull(7, Types.NCHAR);
            pstmt.addBatch();

            pstmt.executeBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb");
        }
    }


    public void checkResult() throws Exception {
        String sql = "SELECT tbname, ts,int_col,long_col,double_col,bool_col,binary_col,nchar_col,int_tag,long_tag,double_tag,bool_tag,binary_tag,nchar_tag FROM example_all_type_stmt1.stb1";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement(); ResultSet resultSet = stmt.executeQuery(sql)) {
            Assert.assertNotNull(resultSet);

            if (resultSet.next()) {
                 if (resultSet.getString(1).equals("ntb")) {
                     assertExceptTimestamp(resultSet, 3);
                 } else {
                     assertAllNullExceptTimestamp(resultSet, 3);
                 }
            }
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
    private void assertAllNullExceptTimestamp(ResultSet rs, int index) throws SQLException {
        Assert.assertNotNull(rs);
        Assert.assertEquals(0, rs.getInt(index++));
        Assert.assertEquals(0, rs.getLong(index++));
        Assert.assertEquals(0, rs.getDouble(index++), 0.0);
        Assert.assertFalse(rs.getBoolean(index++));
        Assert.assertNull(rs.getString(index++));
        Assert.assertNull(rs.getString(index++));

        Assert.assertEquals(0, rs.getInt(index++));
        Assert.assertEquals(0, rs.getLong(index++));
        Assert.assertEquals(0, rs.getDouble(index++), 0.0);
        Assert.assertFalse(rs.getBoolean(index++));
        Assert.assertNull(rs.getString(index++));
        Assert.assertNull(rs.getString(index++));

    }

    private void assertExceptTimestamp(ResultSet rs, int index) throws SQLException {
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.getInt(index++));
        Assert.assertEquals(1000000000000L, rs.getLong(index++));
        Assert.assertEquals(1.1, rs.getDouble(index++), 0.0);
        Assert.assertTrue(rs.getBoolean(index++));
        Assert.assertEquals("binary_value", rs.getString(index++));
        Assert.assertEquals("nchar_value",rs.getString(index++));


        Assert.assertEquals(1, rs.getInt(index++));
        Assert.assertEquals(1000000000000L, rs.getLong(index++));
        Assert.assertEquals(1.1, rs.getDouble(index++), 0.0);
        Assert.assertTrue(rs.getBoolean(index++));
        Assert.assertEquals("binary_value", rs.getString(index++));
        Assert.assertEquals("nchar_value",rs.getString(index++));
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
        System.out.println("testTDengineSource start！");
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/example_all_type_stmt0?user=root&password=taosdata");
        SourceSplitSql sql = new SourceSplitSql("select ts,int_col,long_col,double_col,bool_col,binary_col,nchar_col,int_tag,long_tag,double_tag,bool_tag,binary_tag,nchar_tag,tbname from stb0");
        sourceQuery(sql, 1, connProps);
        System.out.println("testTDengineSource finish！");
    }

    public void sourceQuery(SourceSplitSql sql, int parallelism, Properties connProps) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_STMT2_VERSION, "100.100.100.100");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "example_all_type_stmt1");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "stb1");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/example_all_type_stmt1?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        List<String> fieldNames = Arrays.asList("ts",
                "int_col",
                "long_col",
                "double_col",
                "bool_col",
                "binary_col",
                "nchar_col",
                "int_tag",
                "long_tag",
                "double_tag",
                "bool_tag",
                "binary_tag",
                "nchar_tag",
                "tbname");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, fieldNames);
        input.sinkTo(sink);
        env.execute("flink tdengine source");
        checkResult();
    }

    @Test
    void testNormalTDengineSource() throws Exception {
        System.out.println("testTDengineSource start！");
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/example_all_type_stmt0?user=root&password=taosdata");
        SourceSplitSql sql = new SourceSplitSql("select ts,int_col,long_col,double_col,bool_col,binary_col,nchar_col from stb0");
        sinkToNormal(sql, 1, connProps);
        System.out.println("testTDengineSource finish！");
    }

    public void sinkToNormal(SourceSplitSql sql, int parallelism, Properties connProps) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_STMT2_VERSION, "100.100.100.100");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "example_all_type_stmt1");
        sinkProps.setProperty(TDengineConfigParams.TD_TABLE_NAME, "normal_test");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/example_all_type_stmt1?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        List<String> fieldNames = Arrays.asList("ts",
                "int_col",
                "long_col",
                "double_col",
                "bool_col",
                "binary_col",
                "nchar_col");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, fieldNames);
        input.sinkTo(sink);
        env.execute("flink tdengine source");
        checkResult();
    }


}
