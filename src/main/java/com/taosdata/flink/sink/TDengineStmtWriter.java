package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.flink.sink.entity.TDengineType;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class TDengineStmtWriter<IN> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineSqlWriter.class);
    private final String superTableName;

    private final String normalTableName;
    private final String url;

    private String dbName = "";

    private final Properties properties;

    private final TDengineSinkRecordSerializer<IN> serializer;

    private final List<SinkMetaInfo> sinkMetaInfos;


    private Connection conn;
    private PreparedStatement pstmt;
    private final Lock lock = new ReentrantLock();
    private int batchSize;
    private final AtomicInteger recodeCount = new AtomicInteger(0);

    public TDengineStmtWriter(String url, String dbName, String superTableName, String normalTableName,
                              Properties properties, TDengineSinkRecordSerializer<IN> serializer,
                              List<SinkMetaInfo> sinkMetaInfos, int batchSize) throws SQLException {

        this.superTableName = superTableName;
        this.normalTableName = normalTableName;
        this.url = url;
        this.dbName = dbName;
        this.properties = properties;
        this.serializer = serializer;
        this.sinkMetaInfos = sinkMetaInfos;

        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        initStmt();
    }

    public void initStmt() throws SQLException {
        initConnect();
        String sql = "";
        if (this.sinkMetaInfos != null && this.sinkMetaInfos.size() > 0) {
            if (!Strings.isNullOrEmpty(this.superTableName)) {
                sql = getSuperTableSql();
            } else if (!Strings.isNullOrEmpty(this.normalTableName)) {
                sql = getNormalTableSql();
            }
        }

        if (Strings.isNullOrEmpty(sql)) {
            LOG.info("stmt bind sql exception dbName:{}, superTableName:{}, normalTableName:{}, metaSize:{}",
                    this.dbName, this.superTableName, this.normalTableName, this.sinkMetaInfos.size());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }

        pstmt = conn.prepareStatement(sql);

    }

    public void initConnect() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            this.conn = DriverManager.getConnection(this.url, this.properties);
        } catch (SQLException e) {
            LOG.error("init connect exception error:{}", e.getSQLState());
            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error("init connect exception error:{}", e.getMessage());
            throw new RuntimeException(e);
        }

        LOG.info("connect websocket url ok");
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        List<TDengineSinkRecord> records = serializer.serialize(element, sinkMetaInfos);
        if (records == null || records.isEmpty()) {
            LOG.warn("element serializer result is null!");
            return;
        }

        try {
            lock.lock();
            for (TDengineSinkRecord record : records) {
                setStmtParam(pstmt, record.getColumnParams());
                pstmt.addBatch();
                recodeCount.incrementAndGet();
            }
            if (recodeCount.get() >= batchSize) {
                pstmt.executeBatch();
                recodeCount.set(0);
            }
        } catch (SQLException e) {
            LOG.error("invoke exception info:{}", e.getSQLState());
            throw new IOException(e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        try {
            lock.lock();
            if (recodeCount.get() > 0) {
                pstmt.executeBatch();
                recodeCount.set(0);
            }
        } catch (SQLException e) {
            LOG.error("flush exception info:{}", e.getSQLState());
            throw new IOException(e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        if (this.pstmt != null) {
            this.pstmt.close();
        }
        if (this.conn != null) {
            this.conn.close();
        }
    }

    /**
     * Binding SQL for Splicing Super Tables
     * INSERT INTO suptable (tbname, location, groupId, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)
     * @return
     */
    private String getSuperTableSql() {
        if (Strings.isNullOrEmpty(this.dbName) || Strings.isNullOrEmpty(this.superTableName) || this.sinkMetaInfos.isEmpty()) {
            LOG.warn("StatementData param error");
            return "";
        }

        String sql = "INSERT INTO `" + this.dbName + "`.`" + this.superTableName + "` (";
        sql += String.join(",", sinkMetaInfos.stream().map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") ";

        sql += " VALUES (?";
        for (int i = 1; i < this.sinkMetaInfos.size(); i++) {
            sql += ",?";
        }
        sql += ")";
        LOG.info("stmt bind sql:{}", sql);
        return sql;
    }

    /**
     * Binding SQL for Splicing Normal Tables
     * INSERT INTO tbname (ts, current, voltage, phase) VALUES (?,?,?,?)
     * @return
     */
    private String getNormalTableSql() {
        if (Strings.isNullOrEmpty(this.dbName) || Strings.isNullOrEmpty(this.normalTableName)) {
            LOG.error("NormalTableData param error");
            return "";
        }
        String sql = "INSERT INTO `" + this.dbName + "`.`" + this.normalTableName + "` (" + String.join(",",
                this.sinkMetaInfos.stream().map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") VALUES (?";

        for (int i = 1; i < this.sinkMetaInfos.size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }

    private void setStmtParam(PreparedStatement pstmt, List<Object> columnParams) throws SQLException {
        if (columnParams != null && columnParams.size() > 0) {
            for (int i = 1; i <= this.sinkMetaInfos.size(); i++) {
                SinkMetaInfo metaInfo = this.sinkMetaInfos.get(i - 1);
                switch (metaInfo.getFieldType().getTypeNo()) {
                    case TDengineType.TSDB_DATA_TYPE_BOOL:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.BOOLEAN);
                        } else {
                            pstmt.setBoolean(i, (boolean) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_INT:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.INTEGER);
                        } else {
                            pstmt.setInt(i, (int) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_TINYINT:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.TINYINT);
                        } else {
                            pstmt.setByte(i, (byte) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_TIMESTAMP:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.TIMESTAMP);
                        } else {
                            pstmt.setTimestamp(i, (Timestamp) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_BIGINT:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.BIT);
                        } else {
                            pstmt.setLong(i, (long) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_FLOAT:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.FLOAT);
                        } else {
                            pstmt.setFloat(i, (float) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_DOUBLE:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.DOUBLE);
                        } else {
                            pstmt.setDouble(i, (double) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_SMALLINT:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.SMALLINT);
                        } else {
                            pstmt.setShort(i, (short) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_BINARY:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.BINARY);
                        } else {
                            pstmt.setString(i, (String) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_NCHAR:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.NCHAR);
                        } else {
                            pstmt.setNString(i, (String) columnParams.get(i - 1));
                        }
                        break;
                    case TDengineType.TSDB_DATA_TYPE_GEOMETRY:
                    case TDengineType.TSDB_DATA_TYPE_VARBINARY:
                        if (columnParams.get(i - 1) == null) {
                            pstmt.setNull(i, Types.VARBINARY);
                        } else {
                            pstmt.setBytes(i, (byte[]) columnParams.get(i - 1));
                        }
                        break;

                    default:
                        LOG.error("setStmtLineParams param type is error, type:{}", metaInfo.getFieldType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
                }
            }

        }
    }

}
