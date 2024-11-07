package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.entity.TdengineSourceRecords;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class TdengineSplitReader implements SplitReader<SourceRecord, TdengineSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TdengineSplitReader.class);
    private Properties properties;
    private String sql;
    private volatile boolean running = true;
    private String url;
    private Connection conn;
    private Statement stmt;
    private ResultSetMetaData metaData;
    private ResultSet resultSet;
    private volatile int interval = 0;
    private volatile int batchSize = 2000;
    private int subtaskId;
    public TdengineSplitReader(String url, Properties properties, String sql, SourceReaderContext context) throws ClassNotFoundException {
        this.subtaskId = context.getIndexOfSubtask();;
        this.properties = properties;
        this.sql = sql;
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        this.properties = properties;
        this.url = url;
        LOG.info("init connect websocket ok！");

        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        try {
            this.conn = DriverManager.getConnection(this.url, this.properties);
            this.stmt = this.conn.createStatement();
            this.resultSet = stmt.executeQuery(this.sql);
            this.metaData = resultSet.getMetaData();
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to connect to %s, %sErrMessage: %s%n",
                    this.url,
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
        }

    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        try {
            List<Object> rowData = new ArrayList<>(metaData.getColumnCount());
            while (resultSet.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Object value = resultSet.getObject(i);
                    rowData.add(value);
                }
            }
            SourceRecord sourceRecord = new SourceRecord(rowData);

            SourceRecords sourceRecords = new SourceRecords<>(resultSet.getMetaData(), Arrays.asList(sourceRecord));
            return TdengineSourceRecords.forRecords("" + this.subtaskId, sourceRecords);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TdengineSplit> splitsChange) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }

}
