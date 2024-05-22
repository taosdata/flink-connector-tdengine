package com.taosdata.flink;

import com.taosdata.flink.sink.*;
import com.taosdata.flink.sink.entity.*;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args)  throws Exception {
        SupperTableData supperTableData = new SupperTableData("power");
        supperTableData.setSupperTableName("meters");
        supperTableData.setTagNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        supperTableData.setColumNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        List<SubTableData> subTableDataList = new ArrayList<>();
        for (int i = 1; i <= 2; i++ ) {
            SubTableData subTableData = new SubTableData();
            subTableData.setTableName("d00" + i);

            subTableData.setTagParams(new ArrayList<>(Arrays.asList( new TagParam(DataType.DATA_TYPE_INT, i), new TagParam(DataType.DATA_TYPE_VARCHAR, "California.SanFrancisco"))));

            subTableData.setColumParams(new ArrayList<>(Arrays.asList( new ColumParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                    new ColumParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                    new ColumParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                    new ColumParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
            )));
            subTableDataList.add(subTableData);
        }
        supperTableData.setSubTableDataList(subTableDataList);
        List<String> sqls = new ArrayList<>();
//        sqls.add("CREATE DATABASE IF NOT EXISTS power");
//        sqls.add("USE power");
//        String createTable = "CREATE TABLE IF NOT EXISTS power.test (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)";
//        sqls.add(createTable);

        for (int i = 0; i < 3; i++) {
            String insertQuery = "INSERT INTO " +
                    "power.d100" + i + " USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 219, 0.31000) " +
                    "(NOW + 2a, 12.60000, 218, 0.33000) " +
                    "(NOW + 3a, 12.30000, 221, 0.31000) " +
                    "power.d100"+ (i + 1)+" USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 218, 0.25000) ";
            sqls.add(insertQuery);
        }

        SqlData sqlData = new SqlData("", sqls);

        NormalTableData normalTableData = new NormalTableData("", "test");
        normalTableData.setColumNames(supperTableData.getColumNames());

        normalTableData.setColumParams(new ArrayList<>(Arrays.asList( new ColumParam(DataType.DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new ColumParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new ColumParam(DataType.DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new ColumParam(DataType.DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
        )));
        normalTableData.setTableName("power.test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaosSinkData> dataStream = env.fromElements(TaosSinkData.class, sqlData, supperTableData, normalTableData);
        String url  = "jdbc:TAOS-RS://192.168.1.95:6041/?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        dataStream.addSink(new TaosSinkConnector<TaosSinkData>(url, connProps));
        env.execute("Taos Sink Connector");
    }
}