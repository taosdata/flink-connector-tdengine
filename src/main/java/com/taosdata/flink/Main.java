package com.taosdata.flink;

import com.taosdata.flink.sink.*;
import com.taosdata.flink.sink.entity.*;
import com.taosdata.flink.source.*;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.serializable.TdengineRowDataDeserialization;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.client.program.MiniClusterClient;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.jobgraph.JobGraph;
//import org.apache.flink.runtime.minicluster.MiniCluster;
//import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import java.util.*;

import static org.apache.flink.table.api.Expressions.$;

public class Main {

    public static void main(String[] args)  throws Exception {
        if (args != null && args.length > 0 &&  args[0].equals("init")) {
            initTable();
        } else if (args != null && args.length > 0 && args[0].equals("source")) {
//            testTdSource();
//            testSource();
//            testCdc();
//            testTypeSource();
//            testSourceTypeTable();
//            testSourceTable();
            testTable();
//            testSoureInsert();
        } else {
            insertData();
        }
    }




//    private static void testCdc() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        Properties config = new Properties();
//        config.setProperty("td.connect.type", "ws");
//        config.setProperty("bootstrap.servers", "192.168.1.98:6041");
//        config.setProperty("auto.offset.reset", "earliest");
//        config.setProperty("msg.with.table.name", "true");
//        config.setProperty("enable.auto.commit", "true");
//        config.setProperty("auto.commit.interval.ms", "1000");
//        config.setProperty("group.id", "group7");
//        config.setProperty("client.id", "clinet7");
//        config.setProperty("td.connect.user", "root");
//        config.setProperty("td.connect.pass", "taosdata");
//        config.setProperty("value.deserializer", "com.taosdata.flink.ResultDeserializer");
//        config.setProperty("value.deserializer.encoding", "UTF-8");
//        TaosCdcSource<ResultBean> taosSource= new TaosCdcSource<>("topic_meters", config);
//        DataStreamSource<ConsumerRecords<ResultBean>> src = env.addSource(taosSource);
//        DataStream<String> resultStream = src.map((MapFunction<ConsumerRecords<ResultBean>, String>) records -> {
//            StringBuilder sb = new StringBuilder();
//            for (ConsumerRecord<ResultBean> record : records) {
//                ResultBean resultBean =  record.value();
//                sb.append("ts: " + resultBean.getTs() +
//                    ", current: " + resultBean.getCurrent() +
//                    ", voltage: " + resultBean.getVoltage() +
//                    ", phase: " + resultBean.getPhase());
//                sb.append("\n");
//            }
//
//            taosSource.cancel();
//            return sb.toString();
//        });
//        resultStream.print();
//        env.execute("Flink Table API & SQL TDengine Example");
//    }



    private static void testTdSource() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        SourceSplitSql sql = new SourceSplitSql("ts, `current`, voltage, phase, tbname ", "meters", "", SplitType.SPLIT_TYPE_SQL);
        TdengineSource<RowData> source = new TdengineSource<>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata", connProps, sql, new TdengineRowDataDeserialization());
        DataStreamSource<RowData> input =env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
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


        // 提交作业
        env.execute("xxxxxxxx");

    }


    private static void testSource() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE orders ( ... ) WITH ( 'connector'='mongodb-cdc',... )");
        Table orders = tEnv.from("Orders");
        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));
    }
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        Properties connProps = new Properties();
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        TaosSourceFunction<RowData> taosSourceFunction = new TaosSourceFunction<>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata",
//                connProps, "SELECT ts, `current`, voltage, phase, tbname FROM `meters`");
//        DataStreamSource<List<RowData>> src = env.addSource(taosSourceFunction);
//        src.map((MapFunction<List<RowData>, String>) rowData -> {
//            int count = rowData.size();
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < count; i++) {
//                GenericRowData row = (GenericRowData) rowData.get(i);
//                sb.append("ts: " + row.getField(0) +
//                        ", current: " + row.getFloat(1) +
//                        ", voltage: " + row.getInt(2) +
//                        ", phase: " + row.getFloat(3) +
//                        ", location: " + new String(row.getBinary(4)));
//                sb.append("\n");
//            }
//
//            System.out.println(sb);
//            return sb.toString();
//        });
//        env.execute("Flink Table API & SQL TDengine Example");
//    }
//
//    private static void testTypeSource() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        Properties connProps = new Properties();
//        connProps.setProperty(TaosConnectorParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
//        connProps.setProperty(TaosConnectorParams.PROPERTY_KEY_CHARSET, "UTF-8");
//        connProps.setProperty(TaosConnectorParams.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        TaosSourceFunction<Row> taosSourceFunction = new TaosSourceFunction<Row>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata",
//                connProps, "SELECT ts, `current`, voltage, phase, tbname FROM `meters`") {
//            @Override
//            public Row convert(List<Object> rowData, ResultSetMetaData metaData) throws SQLException {
//                Row row = new Row(rowData.size());
//                for (int i = 0; i < rowData.size(); i++) {
//                    row.setField(i , rowData.get(i));
//                }
//                return row;
//            }
//        };
//
//        DataStreamSource<List<Row>> src = env.addSource(taosSourceFunction);
//        src.map((MapFunction<List<Row>, String>) rowData -> {
//            StringBuilder sb = new StringBuilder();
//            for (Row row : rowData) {
//                sb.append("ts: " + row.getField(0) +
//                        ", current: " + row.getField(1) +
//                        ", voltage: " + row.getField(2) +
//                        ", phase: " + row.getField(3) +
//                        ", location: " + row.getField(4) + "\n");
//            }
//            System.out.println(sb);
//            return sb.toString();
//        });
//
//        env.execute("Flink Table API & SQL TDengine Example");
//    }
//
//    private static void testSourceTable() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        Properties connProps = new Properties();
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        TaosSourceFunction<RowData> taosSourceFunction = new TaosSourceFunction<>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata",
//                connProps, "SELECT ts, `current`, voltage, phase, tbname FROM `meters`");
//        DataStreamSource<List<RowData>> src = env.addSource(taosSourceFunction);
//        // 定义表结构
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
//        tableEnv.fromDataStream(src);
//        tableEnv.createTemporaryView("myTable", src);
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT * FROM `myTable`"
//
//        );
//
//        DataStream<Tuple2<Boolean, RowData>> resultStream = tableEnv.toRetractStream(resultTable, RowData.class);
//        resultStream.print();
//        env.execute("Flink Table API & SQL TDengine Example");
//    }
//
//    private static void testSourceTypeTable() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        Properties connProps = new Properties();
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
//        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        TaosSourceFunction<Row> taosSourceFunction = new TaosSourceFunction<Row>("jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata",
//                connProps, "SELECT ts, `current`, voltage, phase, tbname FROM `meters`") {
//            @Override
//            public Row convert(List<Object> rowData, ResultSetMetaData metaData) throws SQLException {
//                Row row = new Row(rowData.size());
//                for (int i = 0; i < rowData.size(); i++) {
//                    row.setField(i , rowData.get(i));
//                }
//                return row;
//            }
//        };
//        DataStreamSource<List<Row>> src = env.addSource(taosSourceFunction);
//        // 定义表结构
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
//        List<Row> rowType = new ArrayList<>();
//        tableEnv.fromDataStream(src.returns((Class<List<Row>>) rowType.getClass()));
//        Schema schema = TableSchema.builder()
//                .field("ts", DataTypes.TIMESTAMP())
//                .field("current", DataTypes.FLOAT())
//                .field("voltage", DataTypes.INT())
//                .field("phase", DataTypes.FLOAT())
//                .field("tbname", DataTypes.STRING())
//                .build().toSchema();
//
//        tableEnv.createTemporaryView("myTable", src, schema);
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT * FROM `myTable`"
//
//        );
//
//        DataStream<Tuple2<Boolean, List<Row>>> resultStream = tableEnv.toRetractStream(resultTable, (Class<List<Row>>) rowType.getClass());
//        resultStream.print();
//        env.execute("Flink Table API & SQL TDengine Example");
//    }


    public static String printRow(RowData row) {
        System.out.println("-------START------" + row);

        String result = "ts: " + row.getTimestamp(0, 0) +
                ", current: " + row.getFloat(1) +
                ", voltage: " + row.getInt(2) +
                ", phase: " + row.getFloat(3) +
                ", location: " + row.getString(4);

        System.out.println(result);
        return result;
    }
    private static void testTable() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

////        // 创建 TDengine 目录
//        tableEnv.executeSql("CREATE CATALOG tdengine_catalog WITH (" +
//                "  'type'='jdbc'," +
//                "  'default-database'='power'," +
//                "  'base-url'='" + jdbcUrl + "'," +
//                "  'username'='" + username + "'," +
//                "  'password'='" + password + "'," +
//                ")");
//        tableEnv.useCatalog("tdengine_catalog");


        // 注册 TDengine 表到 Flink
//        String tdengineTableDDL = "CREATE TABLE `meters` (" +
//                " ts TIMESTAMP," +
//                " `current` FLOAT," +
//                " voltage INT," +
//                " phase FLOAT" +
//                ") WITH (" +
//                "  'connector' = 'jdbc'," +
//                "  'url' = 'jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata'," +
//                "  'table-name' = 'meters'," +
//                "  'username' = 'root'," +
//                "  'password' = 'taosdata'," +
//                "  'driver' = 'com.taosdata.jdbc.rs.RestfulDriver'" +
//                ")";
//
        String tdengineTableDDL = "CREATE TABLE `meters` (" +
        " ts TIMESTAMP," +
        " `current` FLOAT," +
        " voltage INT," +
        " phase FLOAT," +
        " location STRING" +
        ") WITH (" +
        "  'connector' = 'jdbc'," +
        "  'url' = 'jdbc:TAOS-RS://192.168.1.98:6041/power?user=root&password=taosdata'," +
        "  'table-name' = 'meters'," +
        "  'username' = 'root'," +
        "  'password' = 'taosdata'," +
        "  'scan.query'='SELECT ts, `current`, voltage, phase, location FROM `meters`'," +
        "  'driver' = 'com.taosdata.jdbc.rs.RestfulDriver'" +
        ")";

        tableEnv.executeSql(tdengineTableDDL);
//
//        // 使用 SQL 查询 TDengine 表
        Table resultTable = tableEnv.sqlQuery(
                "SELECT ts, `current`, voltage, phase, location FROM `meters`"
//                "select location, max(`current`) as max_current  from meters group by location"
        );

//
        // 将查询结果转换为 DataStream 并打印
        DataStream<Tuple2<Boolean, RowData>> result = tableEnv.toRetractStream(resultTable, RowData.class);

        DataStream<String> formattedResult = result.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                boolean isRetract = value.f0;
                RowData row = value.f1;

                // 根据需要格式化 Row 的内容
                StringBuilder sb = new StringBuilder();
                if (isRetract) {
                    sb.append("Retract: ");
                } else {
                    sb.append("Add/Update: ");
                }
                String result = printRow(row);
                sb.append(result);

                return sb.toString();
            }
        });

        // 打印结果
        formattedResult.print();
//
//        // 启动 Flink 作业
        env.execute("Flink Table API & SQL TDengine Example");
    }

    private static void testSoureInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> infiniteStream = env.addSource(new TestMakeDataSource("power", "meters", "meters_d00"
                , 1, 1000000, 200000)).setParallelism(1);
        String url  = "jdbc:TAOS-RS://192.168.1.98:6041/?user=root&password=taosdata";
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