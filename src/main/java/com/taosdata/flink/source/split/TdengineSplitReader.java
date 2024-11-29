package com.taosdata.flink.source.split;

import com.google.common.base.Strings;
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

public class TdengineSplitReader implements SplitReader<SourceRecord, TDengineSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TdengineSplitReader.class);
    private Properties properties;
    private volatile boolean running = true;
    private String url;
    private Connection conn;
    private Statement stmt;
    private ResultSetMetaData metaData;
    private ResultSet resultSet;
    private volatile int interval = 0;
    private volatile int batchSize = 2000;
    private int subtaskId;
    private List<TDengineSplit> tdengineSplits;

    private List<TDengineSplit> finishedSplits;

    private Iterator<TDengineSplit> currSplitIter;
    private TDengineSplit currSplit;

    private String currTask;

    private boolean isEnd = false;
    public TdengineSplitReader(String url, Properties properties, SourceReaderContext context) throws ClassNotFoundException, SQLException {
        this.subtaskId = context.getIndexOfSubtask();
        this.finishedSplits = new ArrayList<>();
        tdengineSplits = new ArrayList<>();
        currSplitIter = tdengineSplits.iterator();
        this.properties = properties;
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        this.properties = properties;
        this.url = url;
        LOG.info("init connect websocket ok！");

        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        this.conn = DriverManager.getConnection(this.url, this.properties);
        this.stmt = this.conn.createStatement();

    }
    private SourceRecord getRowData() throws SQLException {
        if (resultSet == null || !resultSet.next()) {
            String task = getSplitTask();
            if (!Strings.isNullOrEmpty(task)) {
                this.resultSet = stmt.executeQuery(task);
                this.metaData = resultSet.getMetaData();
            } else {
                this.resultSet = null;
                return null;
            }
        }

        SourceRecord rowData = new SourceRecord();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Object value = resultSet.getObject(i);
            rowData.addObject(value);
        }
        return rowData;

    }

    private String getSplitTask() {
        if (currSplit != null) {
            setFinishedSplit(currTask);
            currTask = currSplit.getNextTaskSplit();
            if (Strings.isNullOrEmpty(currTask)) {
                finishedSplits.add(currSplit);
                currSplit = null;
            }
        }

        if (Strings.isNullOrEmpty(currTask)) {
            if (this.currSplitIter != null && this.currSplitIter.hasNext()) {
                currSplit = currSplitIter.next();
                currTask = currSplit.getNextTaskSplit();
            }
        }
        return currTask;

    }

    private void setFinishedSplit(String task) {
        if (Strings.isNullOrEmpty(task)) {
            return;
        }

        currSplit.finishTaskSplit(task);
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        try {
            SourceRecords sourceRecords = new SourceRecords();
            for (int i = 0; i < batchSize; i++) {
                SourceRecord sourceRecord = getRowData();
                if (sourceRecord != null) {
                    sourceRecords.addSourceRecord(sourceRecord);
                } else {
                    break;
                }
            }

            if (sourceRecords.getSourceRecordList().isEmpty()) {
                return TdengineSourceRecords.forFinishedSplit("" + this.subtaskId, finishedSplits);
            }
            sourceRecords.setMetaData(this.metaData);
            return  TdengineSourceRecords.forRecords("" + this.subtaskId, sourceRecords, this.tdengineSplits, finishedSplits);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TDengineSplit> splitsChange) {
        List<TDengineSplit> splits = splitsChange.splits();
        this.tdengineSplits.addAll(splits);
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {
        if (this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
        if (this.conn != null) {
            this.conn.close();
            this.conn = null;
        }
    }

}
