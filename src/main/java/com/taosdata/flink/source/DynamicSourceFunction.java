package com.taosdata.flink.source;


import com.taosdata.jdbc.tmq.TaosConsumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.SQLException;
import java.util.Properties;

public class DynamicSourceFunction<T> extends RichParallelSourceFunction<T> {
    private Properties properties;
    private TaosConsumer consumer;
    private volatile boolean running = true;
    public DynamicSourceFunction(Properties properties) {
        this.properties = properties;

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
    public void open(Configuration parameters) throws Exception {
        this.consumer = new TaosConsumer<>(this.properties);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

    }

    public void close() throws Exception {
    }

    @Override
    public void cancel() {

    }
}
