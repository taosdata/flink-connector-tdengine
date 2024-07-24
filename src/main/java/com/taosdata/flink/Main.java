package com.taosdata.flink;

import com.taosdata.flink.sink.*;
import com.taosdata.flink.sink.entity.*;
import com.taosdata.flink.source.TestMakeDataSource;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class Main {
    public static void main(String[] args)  throws Exception {
        if (args != null && args.length > 0 &&  args[0].equals("init")) {
            initTable();
        } else if (args != null && args.length > 0 && args[0].equals("source")) {
            testSoureInsert();
        } else {
            insertData();
        }
    }

    private static void testSoureInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> infiniteStream = env.addSource(new TestMakeDataSource("db", "meters", "meters_d00"
                , 1, 1000000, 200000)).setParallelism(1);
        String url  = "jdbc:TAOS-RS://192.168.1.62:6041/?user=root&password=taosdata";
        infiniteStream.addSink(createTaosSinkConnector(url)).setParallelism(20);
        env.execute("InfiniteSqlSource Interlace Sink");
    }

    private static TaosSinkConnector createTaosSinkConnector(String url) throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        return new TaosSinkConnector<>(url, connProps);
    }

    private  static void initTable() throws Exception {
        String url  = "jdbc:TAOS-RS://192.168.1.98:6041/?user=root&password=taosdata";
        TaosSinkConnector sinkConnector = createTaosSinkConnector(url);
        SqlData sqlData = new SqlData("", getInitDbSqls());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, sqlData);
        dataStream.addSink(sinkConnector).name("TaosSinkConnector");
        env.execute("Taos Sink Connector");
    }
    private static List<String> getInitDbSqls() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("drop database if exists power");
        sqlList.add("CREATE DATABASE IF NOT EXISTS power");
        sqlList.add("USE power");
        sqlList.add("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        sqlList.add("CREATE TABLE IF NOT EXISTS test (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)");
        return sqlList;
    }

    private static void insertData() throws Exception {
        //superTable stmt insert
        SuperTableData superTableData = getSuperTableData();

        //sql insert
        SqlData sqlData = new SqlData("", getSqlData(4000));

        // normal table  stmt insert
        NormalTableData normalTableData = getNormalTableData(superTableData);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, sqlData/*, superTableData, normalTableData*/);

        String url  = "jdbc:TAOS-RS://192.168.1.98:6041/?user=root&password=taosdata";
        TaosSinkConnector sinkConnector = createTaosSinkConnector(url);
        dataStream.addSink(sinkConnector).name("TaosSinkConnector").setParallelism(20);
        env.execute("Taos Sink Connector");
    }
    private static SuperTableData getSuperTableData() {
        SuperTableData superTableData = new SuperTableData("power");
        superTableData.setSuperTableName("meters");
        superTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        superTableData.setColumnNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 1; i <= 2; i++ ) {
            SubTableData subTableData = getSubTableData(i);
            subTableDataList.add(subTableData);
        }
        superTableData.setSubTableDataList(subTableDataList);
        return superTableData;
    }

    private static SubTableData getSubTableData(int i) {
        SubTableData subTableData = new SubTableData();
        subTableData.setTableName("d00" + i);

        subTableData.setTagParams(new ArrayList<>(Arrays.asList( new TagParam(DataType.DATA_TYPE_INT, i), new TagParam(DataType.DATA_TYPE_VARCHAR, "California.SanFrancisco"))));

        subTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
        )));
        return subTableData;
    }

    private static NormalTableData getNormalTableData(SuperTableData superTableData) {
        NormalTableData normalTableData = new NormalTableData("power", "test");
        normalTableData.setColumnNames(superTableData.getColumnNames());
        normalTableData.setColumnParams(new ArrayList<>(Arrays.asList( new ColumnParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new ColumnParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new ColumnParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
        )));
        return normalTableData;
    }

    private static List<String> getSqlData(int nCount) {
        String insertQuery = "";
        long ts = System.currentTimeMillis();
        for (int i = 0; i < nCount; i++) {
            insertQuery += "INSERT INTO " +
                    "power.d100" + i + " USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 219, 0.31000) " +
                    "(NOW + 2a, 12.60000, 218, 0.33000) " +
                    "(NOW + 3a, 12.30000, 221, 0.31000) " +
                    "power.d100"+ (i + 1)+" USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 218, 0.25000);";

        }
        return Collections.singletonList(insertQuery);
    }
}