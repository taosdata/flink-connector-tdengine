package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.sink.entity.*;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class TDengineWriter <IN> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineWriter.class);
    private final String superTableName;

    private final String normalTableName;
    private final String url;

    private String dbName = "";

    private final Properties properties;

    private final TDengineSinkRecordSerializer<IN> serializer;

    private final List<SinkMetaInfo> tagMetaInfos;

    private final List<SinkMetaInfo> columnMetaInfos;

    private Connection conn;
    private TSWSPreparedStatement pstmt;
    private final Lock lock = new ReentrantLock();
    private int batchSize;
    private final AtomicInteger recodeCount = new AtomicInteger(0);

    public TDengineWriter(String url, String dbName, String superTableName, String normalTableName,
                          Properties properties, TDengineSinkRecordSerializer<IN> serializer,
                          List<SinkMetaInfo> tagMetaInfos, List<SinkMetaInfo> columnMetaInfos, int batchSize) throws SQLException {

        this.superTableName = superTableName;
        this.normalTableName = normalTableName;
        this.url = url;
        this.dbName = dbName;
        this.properties = properties;
        this.serializer = serializer;
        this.tagMetaInfos = tagMetaInfos;
        this.columnMetaInfos = columnMetaInfos;

        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        initStmt();
    }

    public void initStmt() throws SQLException {
        initConnect();
        String sql = "";
        if (!Strings.isNullOrEmpty(this.superTableName) && this.tagMetaInfos != null && this.tagMetaInfos.size() > 0) {
            sql = getSuperTableSql();
        } else if (!Strings.isNullOrEmpty(this.normalTableName)  && this.columnMetaInfos != null && this.columnMetaInfos.size() > 0) {
            sql = getNormalTableSql();
        }

        if (Strings.isNullOrEmpty(sql)) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        }

        pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class);

    }

    public void initConnect() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            this.conn = DriverManager.getConnection(this.url, this.properties);
        } catch (SQLException e) {
            LOG.error("open exception error:{}", e.getSQLState());
            throw e;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        LOG.info("connect websocket url ok");
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        List<TDengineSinkRecord> records = serializer.serialize(element);
        if(records == null || records.isEmpty()){
            LOG.warn("element serializer result is null!");
            return;
        }
        for (TDengineSinkRecord record : records) {
            try {
                lock.lock();
                if (!Strings.isNullOrEmpty(record.getTableName())) {
                    pstmt.setTableName(this.dbName + "." + record.getTableName());
                }

                if (record.getTagParams() != null && record.getTagParams().size() > 0) {
                    setStmtTag(pstmt, record.getTagParams());
                }
                setStmtParam(pstmt, record.getColumnParams());
                pstmt.addBatch();
                recodeCount.incrementAndGet();
                pstmt.executeBatch();
                recodeCount.set(0);
            } catch (SQLException e) {
                LOG.error("invoke exception info:{}", e.getSQLState());
                throw new IOException(e.getMessage());
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        int i = 0;
//        try {
//            lock.lock();
//            if (recodeCount.get() > 0) {
//                pstmt.executeBatch();
//                recodeCount.set(0);
//            }
//        } catch (SQLException e) {
//            LOG.error("flush exception info:{}", e.getSQLState());
//            throw new IOException(e.getMessage());
//        } finally {
//            lock.unlock();
//        }
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

    private String getSuperTableSql() {
        if (Strings.isNullOrEmpty(this.dbName) || this.columnMetaInfos == null || this.columnMetaInfos.isEmpty()
                || Strings.isNullOrEmpty(this.superTableName) || this.tagMetaInfos == null || this.tagMetaInfos.isEmpty() ) {
            LOG.warn("StatementData param error");
            return "";
        }

        String sql = "INSERT INTO ? USING `" + this.dbName + "`.`" + this.superTableName + "` (";
        sql += String.join(",", tagMetaInfos.stream().map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") TAGS (?";

        for (int i = 1; i < tagMetaInfos.size(); i++) {
            sql += ",?";
        }

        sql += ") (" + String.join(",", this.columnMetaInfos.stream().map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") VALUES (?";
        for (int i = 1; i < this.columnMetaInfos.size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }

    private String getNormalTableSql() {
        if (Strings.isNullOrEmpty(this.dbName) || Strings.isNullOrEmpty(this.normalTableName)
                ||this.columnMetaInfos == null || this.columnMetaInfos.isEmpty()) {
            LOG.error("NormalTableData param error");
            return "";
        }
        String sql = "INSERT INTO ? (" + String.join(",", this.columnMetaInfos.stream().map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") VALUES (?";
        for (int i = 1; i < this.columnMetaInfos.size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }

    private void setStmtTag(TSWSPreparedStatement pstmt, List<Object> tagParams) throws SQLException {
        if (tagParams != null && tagParams.size() > 0) {
            for (int i = 1; i <= tagParams.size(); i++) {
                SinkMetaInfo metaInfo = tagMetaInfos.get(i - 1);
                switch (metaInfo.getFieldType().getTypeNo()) {
                    case TaosType.TSDB_DATA_TYPE_BOOL:
                        pstmt.setTagBoolean(i, (boolean) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_INT:
                        pstmt.setTagInt(i, (int) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_TINYINT:
                        pstmt.setTagByte(i, (byte) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                        pstmt.setTagTimestamp(i, (long) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BIGINT:
                        pstmt.setTagLong(i, (long) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_FLOAT:
                        pstmt.setTagFloat(i, (float) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_DOUBLE:
                        pstmt.setTagDouble(i, (double) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_SMALLINT:
                        pstmt.setTagShort(i, (short) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BINARY:
                        pstmt.setTagString(i, (String) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_JSON:
                        pstmt.setTagJson(i, (String) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_NCHAR:
                        pstmt.setTagNString(i, (String) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                        pstmt.setTagGeometry(i, (byte[]) tagParams.get(i - 1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_VARBINARY:
                        pstmt.setTagVarbinary(i, (byte[]) tagParams.get(i - 1));
                        break;
                    default:
                        LOG.error("setStmtTag tag type is error, type:{}", metaInfo.getFieldType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE);
                }
            }
        }
    }

    private void setStmtParam(TSWSPreparedStatement pstmt, List<Object> columnParams) throws SQLException {
        if (columnParams != null && columnParams.size() > 0) {
            for (int i = 1; i <= columnMetaInfos.size(); i++) {
                SinkMetaInfo metaInfo = columnMetaInfos.get(i - 1);
                switch (metaInfo.getFieldType().getTypeNo()) {
                    case TaosType.TSDB_DATA_TYPE_BOOL:
                        pstmt.setBoolean(i, (boolean) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_INT:
                        pstmt.setInt(i, (int) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_TINYINT:
                        pstmt.setByte(i, (byte) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                        pstmt.setTimestamp(i, (Timestamp) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BIGINT:
                        pstmt.setLong(i, (long) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_FLOAT:
                        pstmt.setFloat(i, (float) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_DOUBLE:
                        pstmt.setDouble(i, (double) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_SMALLINT:
                        pstmt.setShort(i, (short) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BINARY:
                        pstmt.setString(i, (String) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_NCHAR:
                        pstmt.setNString(i, (String) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                        pstmt.setGeometry(i, (byte[]) columnParams.get(i -1));
                        break;
                    case TaosType.TSDB_DATA_TYPE_VARBINARY:
                        pstmt.setVarbinary(i, (byte[]) columnParams.get(i -1));
                        break;
                    default:
                        LOG.error("setStmtLineParams param type is error, type:{}", metaInfo.getFieldType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
                }
            }

        }
    }

}
