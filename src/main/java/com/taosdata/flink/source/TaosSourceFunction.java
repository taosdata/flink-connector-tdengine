//package com.taosdata.flink.source;
//
//import com.taosdata.flink.sink.function.TaosSinkConnector;
//import com.taosdata.flink.source.entity.TaosRowConverter;
//import com.taosdata.jdbc.TSDBDriver;
//import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//import org.apache.flink.table.data.GenericRowData;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//
//public class TaosSourceFunction<T> extends RichParallelSourceFunction<List<T>> {
//    private static final Logger LOG = LoggerFactory.getLogger(TaosSinkConnector.class);
//    private Properties properties;
//    private String sql;
//    private volatile boolean running = true;
//    private TaosRowConverter rowConverter;
//    private String url;
//    private Connection conn;
//    private volatile int interval = 0;
//    private volatile int batchSize = 2000;
//    public TaosSourceFunction(String url, Properties properties, String sql) {
//
//        this.properties = properties;
//        this.sql = sql;
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
//        this.properties = properties;
//        this.url = url;
//        LOG.info("init connect websocket ok！");
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
//        try (Connection conn = DriverManager.getConnection(this.url, this.properties)) {
//            this.conn = conn;
//        } catch (Exception ex) {
//            // please refer to the JDBC specifications for detailed exceptions info
//            System.out.printf("Failed to connect to %s, %sErrMessage: %s%n",
//                    this.url,
//                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
//                    ex.getMessage());
//            ex.printStackTrace();
//            throw ex;
//        }
//    }
//
//    public T convert(List<Object> rowData, ResultSetMetaData metaData) throws SQLException {
//        return (T) GenericRowData.of(rowData.toArray());
//    }
//
//    @Override
//    public void run(SourceContext<List<T>> sourceContext) throws Exception {
//        if (null == this.conn || this.conn.isClosed()) {
//            this.conn = DriverManager.getConnection(this.url, this.properties);
//            LOG.info("invoke connect websocket url:" + this.url);
//        }
//
//        try (Statement stmt = this.conn.createStatement();
//            ResultSet resultSet = stmt.executeQuery(this.sql)) {
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            List<T> resoults = new ArrayList<>();
//            int count = 0;
//            while (resultSet.next()) {
//                List<Object> rowData = new ArrayList<>(metaData.getColumnCount());
//                for (int i = 1; i <= metaData.getColumnCount(); i++) {
//                    Object value = resultSet.getObject(i);
//                    rowData.add(value);
//                }
//                T resoult = convert(rowData, metaData);
//                resoults.add(resoult);
//                count++;
//                if (count == this.batchSize) {
//                    sourceContext.collect(resoults);
//                    if (this.interval > 0) {
//                        Thread.sleep(this.interval);
//                    }
//                    resoults.clear();
//                    count = 0;
//                }
//
//
//            }
//            if (!resoults.isEmpty()) {
//                sourceContext.collect(resoults);
//            }
//
//        } catch (Exception ex) {
//            // please refer to the JDBC specifications for detailed exceptions info
//            System.out.printf("Failed to query data from power.meters, %sErrMessage: %s%n",
//                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
//                    ex.getMessage());
//            // Print stack trace for context in examples. Use logging in production.
//            ex.printStackTrace();
//            throw ex;
//        } finally {
//            close();
//        }
//    }
//
//    public void close() throws Exception {
//        if (conn != null) {
//            conn.close();
//            conn = null;
//        }
//        super.close();
//    }
//    public void adjustInterval(int interval) {
//        this.interval = interval;
//    }
//
//    public void adjustBatchSize(int batchSize) {
//        this.batchSize = batchSize;
//    }
//
//    @Override
//    public void cancel() {
//    }
//}
