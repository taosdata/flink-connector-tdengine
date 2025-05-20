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

import java.util.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TDFlinkSourceAllTypesTest {
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

    private void init(Connection conn) throws SQLException {
        schemaList = new ArrayList<>();
        schemaList.add("DROP TOPIC IF EXISTS topic_table_all_type_stmt");
        schemaList.add("drop database if exists example_all_type_stmt0");
        schemaList.add("drop database if exists example_all_type_stmt1");
        schemaList.add("CREATE DATABASE IF NOT EXISTS example_all_type_stmt0");
        schemaList.add("CREATE DATABASE IF NOT EXISTS example_all_type_stmt1");

        for (int i = 1; i >= 0; i--) {
            schemaList.add("USE example_all_type_stmt" + i);
            String table = "CREATE STABLE IF NOT EXISTS stb" + i +
                    "(ts TIMESTAMP, " +
                    "int_col INT, " +
                    "long_col BIGINT, " +
                    "double_col DOUBLE, " +
                    "bool_col BOOL, " +
                    "binary_col BINARY(100), " +
                    "nchar_col NCHAR(100), " +
                    "varbinary_col VARBINARY(100), " +
                    "geometry_col GEOMETRY(100)," +
                    "tinyint_col TINYINT, " +
                    "smallint_col SMALLINT) " +
                    "tags (" +
                    "int_tag INT, " +
                    "long_tag BIGINT, " +
                    "double_tag DOUBLE, " +
                    "bool_tag BOOL, " +
                    "binary_tag BINARY(100), " +
                    "nchar_tag NCHAR(100), " +
                    "varbinary_tag VARBINARY(100), " +
                    "geometry_tag GEOMETRY(100), " +
                    "tinyint_tag TINYINT, " +
                    "smallint_tag SMALLINT)";
            schemaList.add(table);
        }

        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < schemaList.size(); i++) {
                stmt.execute(schemaList.get(i));
            }
        }
    }

    private void stmtAll(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb0 tags(?,?,?,?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            // set table name
            pstmt.setTableName("ntb");
            // set tags
            pstmt.setTagInt(0, 1);
            pstmt.setTagLong(1, 1000000000000L);
            pstmt.setTagDouble(2, 1.1);
            pstmt.setTagBoolean(3, true);
            pstmt.setTagString(4, "binary_value");
            pstmt.setTagNString(5, "nchar_value");
            pstmt.setTagVarbinary(6, new byte[]{(byte) 0x98, (byte) 0xf4, 0x6e});
            pstmt.setTagGeometry(7, new byte[]{
                    0x01, 0x01, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59,
                    0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40});
            pstmt.setTagByte(8, (byte) 9);
            pstmt.setTagShort(9, (short) 13);

            long current = System.currentTimeMillis();

            pstmt.setTimestamp(1, new Timestamp(current));
            pstmt.setInt(2, 1);
            pstmt.setLong(3, 1000000000000L);
            pstmt.setDouble(4, 1.1);
            pstmt.setBoolean(5, true);
            pstmt.setString(6, "binary_value");
            pstmt.setNString(7, "nchar_value");
            pstmt.setVarbinary(8, new byte[]{(byte) 0x98, (byte) 0xf4, 0x6e});
            pstmt.setGeometry(9, new byte[]{
                    0x01, 0x01, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59,
                    0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40});
            pstmt.setByte(10, (byte) 9);
            pstmt.setShort(11, (short) 13);


            pstmt.addBatch();
            pstmt.executeBatch();

            pstmt.setTableName("w3");
            pstmt.setTagNull(0, TSDBConstants.TSDB_DATA_TYPE_INT);
            pstmt.setTagNull(1, TSDBConstants.TSDB_DATA_TYPE_BIGINT);
            pstmt.setTagNull(2, TSDBConstants.TSDB_DATA_TYPE_DOUBLE);
            pstmt.setTagNull(3, TSDBConstants.TSDB_DATA_TYPE_BOOL);
            pstmt.setTagNull(4, TSDBConstants.TSDB_DATA_TYPE_BINARY);
            pstmt.setTagNull(5, TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            pstmt.setTagNull(6, TSDBConstants.TSDB_DATA_TYPE_VARBINARY);
            pstmt.setTagNull(7, TSDBConstants.TSDB_DATA_TYPE_GEOMETRY);
            pstmt.setTagNull(8, TSDBConstants.TSDB_DATA_TYPE_TINYINT);
            pstmt.setTagNull(9, TSDBConstants.TSDB_DATA_TYPE_SMALLINT);

            pstmt.setTimestamp(1, new Timestamp(current + 1));
            pstmt.setNull(2, Types.INTEGER);
            pstmt.setNull(3, Types.BIGINT);
            pstmt.setNull(4, Types.DOUBLE);
            pstmt.setNull(5, Types.BOOLEAN);
            pstmt.setNull(6, Types.BINARY);
            pstmt.setNull(7, Types.NCHAR);
            pstmt.setNull(8, Types.VARBINARY);
            pstmt.setNull(9, Types.VARBINARY);
            pstmt.setNull(10, Types.TINYINT);
            pstmt.setNull(11, Types.SMALLINT);
            pstmt.addBatch();

            pstmt.executeBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb");
        }
    }

    public void checkResult() throws Exception {
        String sql = "SELECT tbname, ts,int_col,long_col,double_col,bool_col,binary_col,nchar_col,varbinary_col,geometry_col, tinyint_col, smallint_col,int_tag,long_tag,double_tag,bool_tag,binary_tag,nchar_tag,varbinary_tag,geometry_tag, tinyint_tag, smallint_tag FROM example_all_type_stmt1.stb1";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement(); ResultSet resultSet = stmt.executeQuery(sql)) {
            Assert.assertNotNull(resultSet);

            while (resultSet.next()) {
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
        Assert.assertNull(rs.getBytes(index++));
        Assert.assertNull(rs.getBytes(index++));
        Assert.assertEquals(0, rs.getByte(index++));
        Assert.assertEquals(0, rs.getShort(index++));

        Assert.assertEquals(0, rs.getInt(index++));
        Assert.assertEquals(0, rs.getLong(index++));
        Assert.assertEquals(0, rs.getDouble(index++), 0.0);
        Assert.assertFalse(rs.getBoolean(index++));
        Assert.assertNull(rs.getString(index++));
        Assert.assertNull(rs.getString(index++));
        Assert.assertNull(rs.getBytes(index++));
        Assert.assertNull(rs.getBytes(index++));
        Assert.assertEquals(0, rs.getByte(index++));
        Assert.assertEquals(0, rs.getShort(index++));
    }

    private void assertExceptTimestamp(ResultSet rs, int index) throws SQLException {
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.getInt(index++));
        Assert.assertEquals(1000000000000L, rs.getLong(index++));
        Assert.assertEquals(1.1, rs.getDouble(index++), 0.0);
        Assert.assertTrue(rs.getBoolean(index++));
        Assert.assertEquals("binary_value", rs.getString(index++));
        Assert.assertEquals("nchar_value",rs.getString(index++));
        Assert.assertNotNull(rs.getBytes(index++));
        Assert.assertNotNull(rs.getBytes(index++));
        Assert.assertEquals((byte) 9, rs.getByte(index++));
        Assert.assertEquals((short) 13,rs.getShort(index++));

        Assert.assertEquals(1, rs.getInt(index++));
        Assert.assertEquals(1000000000000L, rs.getLong(index++));
        Assert.assertEquals(1.1, rs.getDouble(index++), 0.0);
        Assert.assertTrue(rs.getBoolean(index++));
        Assert.assertEquals("binary_value", rs.getString(index++));
        Assert.assertEquals("nchar_value",rs.getString(index++));
        Assert.assertNotNull(rs.getBytes(index++));
        Assert.assertNotNull(rs.getBytes(index++));
        Assert.assertEquals((byte) 9, rs.getByte(index++));
        Assert.assertEquals((short) 13,rs.getShort(index++));
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
        SourceSplitSql sql = new SourceSplitSql("select ts,int_col,long_col,double_col,bool_col,binary_col,nchar_col,varbinary_col,geometry_col, tinyint_col, smallint_col,int_tag,long_tag,double_tag,bool_tag,binary_tag,nchar_tag,varbinary_tag,geometry_tag, tinyint_tag, smallint_tag, tbname from stb0");
        sourceQuery(sql, 1, connProps);
        System.out.println("testTDengineSource finish！");
    }

    public void sourceQuery(SourceSplitSql sql, int parallelism, Properties connProps) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
//        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
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
                "varbinary_col",
                "geometry_col",
                "tinyint_col",
                "smallint_col",
                "int_tag",
                "long_tag",
                "double_tag",
                "bool_tag",
                "binary_tag",
                "nchar_tag",
                "varbinary_tag",
                "geometry_tag",
                "tinyint_tag",
                "smallint_tag",
                "tbname");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, fieldNames);
        input.sinkTo(sink);
        env.execute("flink tdengine source");
        checkResult();
    }


}
