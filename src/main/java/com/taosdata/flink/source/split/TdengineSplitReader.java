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
        this.tdengineSplits = new ArrayList<>();
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
        try {
            if (resultSet == null || !resultSet.next()) {
                if (resultSet != null) {
                    resultSet.close();
                    resultSet = null;
                }

                while (initNextSplitTask()) {
                    this.resultSet = stmt.executeQuery(currTask);
                    if (this.resultSet.next()) {
                        this.metaData = resultSet.getMetaData();
                        break;
                    }
                    resultSet.close();
                    resultSet = null;
                }
            }
            if (resultSet != null) {
                SourceRecord rowData = new SourceRecord(resultSet.getMetaData(), this.currSplit.getFinishList());
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Object value = resultSet.getObject(i);
                    rowData.addObject(value);
                }
                return rowData;
            }
            return null;
        } catch (Exception e) {
           throw e;
        }


    }

    private boolean initNextSplitTask() throws SQLException {
        setFinishedSplit(currTask);
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }

        currTask = "";
        if (currSplit != null) {
            currTask = currSplit.getNextTaskSplit();
        }

        if (Strings.isNullOrEmpty(currTask)) {
            return false;
        }
        return true;

    }

    private void initNextSplit() {
        if (Strings.isNullOrEmpty(currTask)) {
            if (currSplit != null) {
                finishedSplits.add(currSplit);
            }
            currSplit = null;
            if (this.currSplitIter != null && this.currSplitIter.hasNext()) {
                currSplit = this.currSplitIter.next();
            }
        }

    }

    private void setFinishedSplit(String task) {
        if (Strings.isNullOrEmpty(task)) {
            return;
        }

        currSplit.addFinishTaskSplit(task);
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        try {
            initNextSplit();
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
                return TdengineSourceRecords.forFinishedSplit(finishedSplits);
            }

            sourceRecords.setMetaData(this.metaData);
            return  TdengineSourceRecords.forRecords(currSplit.splitId, sourceRecords, finishedSplits);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TDengineSplit> splitsChange) {
        List<TDengineSplit> splits = splitsChange.splits();
        this.tdengineSplits.addAll(splits);
        currSplitIter = this.tdengineSplits.iterator();
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
