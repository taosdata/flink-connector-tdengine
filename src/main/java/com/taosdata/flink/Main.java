package com.taosdata.flink;

import com.taosdata.flink.sink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args)  throws Exception {
        StatementData statementData = new StatementData();
        statementData.setDbName("power");
        statementData.setSupperTableName("meters");
        statementData.setTableName("d001");
        statementData.setTagFieldNames(new ArrayList<>(Arrays.asList("groupId", "location")));
        statementData.setTagParams(new ArrayList<>(Arrays.asList( new Param(DataType.TSDB_DATA_TYPE_INT, 1), new Param(DataType.TSDB_DATA_TYPE_VARCHAR, "California.SanFrancisco"))));
        statementData.setFieldNames(new ArrayList<>(Arrays.asList("ts", "current", "voltage", "phase")));
        statementData.setMode(1);
        statementData.setColumParams(new ArrayList<>(Arrays.asList( new StatementParam(DataType.TSDB_DATA_TYPE_TIMESTAMP, new ArrayList<Long>(Arrays.asList(1709183268577L, 1709183268578L, 1709183268579L))),
                new StatementParam(DataType.TSDB_DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(10.2f, 10.3f, 10.4f))),
                new StatementParam(DataType.TSDB_DATA_TYPE_INT, new ArrayList<>(Arrays.asList(292, 293, 294))),
                new StatementParam(DataType.TSDB_DATA_TYPE_FLOAT, new ArrayList<>(Arrays.asList(0.32f, 0.33f, 0.34f)))
                )));
//        List<List<Param>> lineParams = new ArrayList<>();
//        for (int i = 0; i < 4; i++) {
//            List<Param> lineParam = new ArrayList<>();
//            lineParam.add(new Param(DataType.TSDB_DATA_TYPE_TIMESTAMP, 1709183268567L + i));
//            lineParam.add(new Param(DataType.TSDB_DATA_TYPE_FLOAT, 10.2f + i));
//            lineParam.add(new Param(DataType.TSDB_DATA_TYPE_INT, 292 + i));
//            lineParam.add(new Param(DataType.TSDB_DATA_TYPE_FLOAT, 0.32f + i));
//            lineParams.add(lineParam);
//        }

//        statementData.setLineParams(lineParams);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StatementData> dataStream = env.fromElements(statementData);
        TaosOptions taosOptions = new TaosOptions("root", "taosdata", "192.168.1.95", 6041);
        dataStream.addSink(new TaosSinkFunction<StatementData>(taosOptions));
        env.execute("Dynamic Sink Function");


    }
}