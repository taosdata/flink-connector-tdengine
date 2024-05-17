package com.taosdata.flink.source;

import com.taosdata.flink.sink.TaosOptions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class DynamicSourceFunction<T> extends RichParallelSourceFunction<T> {
    private TaosOptions taosOptions;
    public DynamicSourceFunction(TaosOptions taosOptions) {
//        this.taosOptions = taosOptions;
//        final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;
//        Connection connection;
//
//// get connection
//        Properties properties = new Properties();
//        properties.setProperty("charset", "UTF-8");
//        properties.setProperty("locale", "en_US.UTF-8");
//        properties.setProperty("timezone", "UTC-8");
//        System.out.println("get connection starting...");
//        connection = DriverManager.getConnection(url, properties);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
