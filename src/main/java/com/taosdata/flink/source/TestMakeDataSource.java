package com.taosdata.flink.source;

import com.taosdata.flink.sink.entity.SqlData;
import com.taosdata.flink.sink.entity.TaosSinkData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TestMakeDataSource  implements SourceFunction<TaosSinkData> {
    private final AtomicLong counter = new AtomicLong(0);
    private final String stbName;
    private final String dbName;
    private final int interlace;
    private final int subTableCount;
    private final String subTablePrefix;
    private final int rows;
    public TestMakeDataSource(String dbName, String stbName, String subTablePrefix, int interlace, int subTableCount, int rows) {
        this.stbName = stbName;
        this.subTablePrefix = subTablePrefix;
        this.interlace = interlace;
        this.subTableCount = subTableCount;
        this.rows = rows;
        this.dbName = dbName;
    }


    @Override
    public void run(SourceContext<TaosSinkData> sourceContext) throws Exception {

        long ts = System.currentTimeMillis();
        int count = 0;
        for (int j = 0; j < rows / interlace; j++) {
            for (int i = 0; i < subTableCount; i++) {
                String subTableName = subTablePrefix + i;
                StringBuilder insertSQL = new StringBuilder("insert into `" + subTableName + "` USING "+ this.dbName +"."+ this.stbName + " (`groupid` , `location`) TAGS (" + i + ", \"ddddsssssddd\" ) values ");
                StringBuilder appendSuffix = new StringBuilder();
                for (int k = 0; k < interlace; k++) {
                    appendSuffix.append("(")
                            .append(ts + count).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(",")
                            .append(j).append(")");
                    counter.incrementAndGet();
                    count++;
                }
                insertSQL.append(appendSuffix);
                List<String> sqlList = new ArrayList<>();
                sqlList.add(insertSQL.toString());
                String benchSqls = String.join(";", sqlList);
                SqlData newSqlData = new SqlData(this.dbName, Collections.singletonList(benchSqls));
                System.out.println("data counter:" + counter.get());
                sourceContext.collect(newSqlData);
            }
        }
        sourceContext.close();
    }

    @Override
    public void cancel() {

    }
}
