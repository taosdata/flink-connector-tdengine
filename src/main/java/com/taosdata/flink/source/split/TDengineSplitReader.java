package com.taosdata.flink.source.split;

import com.google.common.base.Strings;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.source.entity.SourceRecords;
import com.taosdata.flink.source.entity.SplitResultRecord;
import com.taosdata.flink.source.entity.SplitResultRecords;
import com.taosdata.flink.source.entity.TDengineSourceRecordsWithSplitsIds;
import com.taosdata.flink.source.serializable.TDengineRecordDeserialization;
import com.taosdata.flink.source.serializable.TDengineRowDataDeserialization;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.Utils;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class TDengineSplitReader<OUT> implements SplitReader<SplitResultRecords<OUT>, TDengineSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineSplitReader.class);
    private Properties properties;
    private String url;
    private Connection conn;
    private Statement stmt;
    private ResultSetMetaData metaData;
    private ResultSet resultSet;

    private volatile int batchSize;
    private int subtaskId;
    private List<TDengineSplit> tdengineSplits;

    private List<String> finishedSplits;

    private Iterator<TDengineSplit> currSplitIter;
    private TDengineSplit currSplit;

    private String currTask;

    private TDengineRecordDeserialization<OUT> tdengineRecordDeserialization;

    private boolean isEnd = false;

    public TDengineSplitReader(Properties properties, SourceReaderContext context) throws ClassNotFoundException, SQLException {
        LOG.debug("create TDengineSplitReader object properties:{}", properties);
        this.subtaskId = context.getIndexOfSubtask();
        this.finishedSplits = new ArrayList<>();
        this.tdengineSplits = new ArrayList<>();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        this.properties = properties;
        this.url = this.properties.getProperty(TDengineConfigParams.TD_JDBC_URL, "");


        Class.forName("com.taosdata.jdbc.ws.WebSocketDriver");
        this.conn = DriverManager.getConnection(this.url, this.properties);
        this.stmt = this.conn.createStatement();
        String outType = this.properties.getProperty(TDengineConfigParams.VALUE_DESERIALIZER, "");
        if (outType.compareTo("RowData") == 0) {
            tdengineRecordDeserialization = (TDengineRecordDeserialization<OUT>) new TDengineRowDataDeserialization();
        } else {
            tdengineRecordDeserialization = (TDengineRecordDeserialization<OUT>) Utils.newInstance(Utils.parseClassType(outType));
        }
        String strBatchSize = properties.getProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");
        batchSize = Integer.parseInt(strBatchSize);
        LOG.debug("intit TDengineSplitReader ok!");
    }

    private SplitResultRecord getRowData() throws SQLException {
        try {
            // Determine whether the result set has been pulled successfully
            if (resultSet == null || !resultSet.next()) {

                if (resultSet != null) {
                    resultSet.close();
                    resultSet = null;
                }
                // get next split tast
                while (initNextSplitTask()) {
                    LOG.debug("executeQuery get resultSet, sql:{}!", currTask);
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
                SplitResultRecord rowData = new SplitResultRecord(resultSet.getMetaData());
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Object value = resultSet.getObject(i);
                    rowData.addObject(value);
                }
                LOG.debug("from resultSet get rowData, sql:{}!", currTask);
                return rowData;
            }
            LOG.debug("from resultSet no get rowData, sql:{}!", currTask);
            return null;
        } catch (Exception e) {
            LOG.error("get rowdata excption:{}, currTask:{}", e.toString(), currTask);
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
            LOG.debug("get split task, sql:{}!", currTask);
            return false;
        }
        LOG.debug("not get split task!");
        return true;

    }

    private void initNextSplit() {
        if (Strings.isNullOrEmpty(currTask)) {
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
    public RecordsWithSplitIds<SplitResultRecords<OUT>> fetch() throws IOException {
        try {
            initNextSplit();
            SourceRecords<OUT> sourceRecords = new SourceRecords<>();
            for (int i = 0; i < batchSize; i++) {
                SplitResultRecord splitResultRecord = getRowData();
                if (splitResultRecord != null) {
                    sourceRecords.addSourceRecord(tdengineRecordDeserialization.convert(splitResultRecord));
                } else {
                    if (currSplit != null && !Strings.isNullOrEmpty(currSplit.splitId)) {
                        finishedSplits.add(currSplit.splitId);
                    }
                    break;
                }
            }

            if (sourceRecords.isEmpty()) {
                LOG.info("source fetch fininshed!");
                return TDengineSourceRecordsWithSplitsIds.forFinishedSplit(finishedSplits);
            }

            SplitResultRecords splitResultRecords = new SplitResultRecords();
            splitResultRecords.setMetaData(this.metaData);
            splitResultRecords.setTDengineSplit(currSplit);
            splitResultRecords.setSourceRecords(sourceRecords);
            LOG.debug("get split task rowdata, count:{}", sourceRecords.getRecords().size());
            return TDengineSourceRecordsWithSplitsIds.forRecords(currSplit.splitId, splitResultRecords);
        } catch (SQLException e) {
            LOG.error("source fetch excption:{}, currTask:{}", e.toString(), currTask);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("source fetch excption:{}, currTask:{}", e.toString(), currTask);
            throw new RuntimeException(e);
        }
    }

    /**
     * Handle the split changes. This call should be non-blocking.
     *
     * @param splitsChange the split changes that the SplitReader needs to handle.
     */
    @Override
    public void handleSplitsChanges(SplitsChange<TDengineSplit> splitsChange) {
        LOG.debug("received split task ！");
        List<TDengineSplit> splits = splitsChange.splits();
        this.tdengineSplits.addAll(splits);
        currSplitIter = this.tdengineSplits.iterator();
    }

    @Override
    public void wakeUp() {
        LOG.debug("source reader {} wakeUp!", subtaskId);
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
        LOG.debug("source reader {} close!", subtaskId);
    }

}
