package com.taosdata.flink.sink;

import com.google.common.base.Strings;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.flink.sink.entity.TaosType;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class TDengineSqlWriter<IN> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineSqlWriter.class);
    private final String superTableName;

    private final String normalTableName;
    private final String url;

    private String dbName = "";

    private final Properties properties;

    private final TDengineSinkRecordSerializer<IN> serializer;

    private final List<SinkMetaInfo> sinkMetaInfos;

    private Connection conn;

    private Statement statement;
    private String sqlPrefix;
    private StringBuilder executeSqls;

    private static final int ONE_MILLION = 1000000;
    private final Lock lock = new ReentrantLock();

    public TDengineSqlWriter(String url, String dbName, String superTableName, String normalTableName,
                             Properties properties, TDengineSinkRecordSerializer<IN> serializer,
                             List<SinkMetaInfo> sinkMetaInfos) throws SQLException {

        this.superTableName = superTableName;
        this.normalTableName = normalTableName;
        this.url = url;
        this.dbName = dbName;
        this.properties = properties;
        this.serializer = serializer;
        this.sinkMetaInfos = sinkMetaInfos;
        executeSqls = new StringBuilder();
        initConnect();
    }

    public void initConnect() throws SQLException {
        try {
            if (Strings.isNullOrEmpty(this.dbName) || this.sinkMetaInfos == null || this.sinkMetaInfos.isEmpty()) {
                LOG.warn("StatementData param error");
                throw new RuntimeException("StatementData param error");
            }
            if (!Strings.isNullOrEmpty(this.superTableName)) {
                sqlPrefix = "INSERT INTO `" + this.dbName + "`.`"
                        + this.superTableName + "` ("
                        + String.join(",", sinkMetaInfos.stream()
                        .map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") VALUES ";

            } else if (!Strings.isNullOrEmpty(this.normalTableName)) {
                sqlPrefix = "INSERT INTO `" + this.dbName + "`.`"
                        + this.normalTableName + "` ("
                        + String.join(",", sinkMetaInfos.stream()
                        .map(SinkMetaInfo::getFieldName).collect(Collectors.toList())) + ") VALUES ";
            }
            executeSqls.append(sqlPrefix);
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            this.conn = DriverManager.getConnection(this.url, this.properties);
            statement = this.conn.createStatement();

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
        List<TDengineSinkRecord> records = serializer.serialize(element, sinkMetaInfos);
        if(records == null || records.isEmpty()){
            LOG.warn("element serializer result is null!");
            return;
        }

        try {
            lock.lock();
            executeTableSql(records);
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
            if (executeSqls.length() > sqlPrefix.length()) {
                statement.executeUpdate(executeSqls.toString());
                executeSqls.setLength(0);
                executeSqls.append(sqlPrefix);
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
        if (this.statement != null) {
            this.statement.close();
        }
        if (this.conn != null) {
            this.conn.close();
        }
    }

    private void executeTableSql(List<TDengineSinkRecord> records) throws SQLException, UnsupportedEncodingException {
        for (TDengineSinkRecord sinkRecord:records) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < this.sinkMetaInfos.size(); i++) {
                sb.append(getStringParam(sinkRecord.getColumnParams().get(i), sinkMetaInfos.get(i).getFieldType().getTypeNo()));
                if (i < sinkMetaInfos.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")") ;
            if ((executeSqls.length() + sb.length()) > ONE_MILLION) {
                statement.executeUpdate(executeSqls.toString());
                executeSqls.setLength(0);
                executeSqls.append(sqlPrefix);
            }
            executeSqls.append(sb);
        }
    }

//    private void setStmtTag(TSWSPreparedStatement pstmt, List<Object> tagParams) throws SQLException {
//        if (tagParams != null && tagParams.size() > 0) {
//            for (int i = 1; i <= tagParams.size(); i++) {
//                SinkMetaInfo metaInfo = tagMetaInfos.get(i - 1);
//                switch (metaInfo.getFieldType().getTypeNo()) {
//                    case TaosType.TSDB_DATA_TYPE_BOOL:
//                        pstmt.setTagBoolean(i, (boolean) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_INT:
//                        pstmt.setTagInt(i, (int) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_TINYINT:
//                        pstmt.setTagByte(i, (byte) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
//                        pstmt.setTagTimestamp(i, (long) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_BIGINT:
//                        pstmt.setTagLong(i, (long) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_FLOAT:
//                        pstmt.setTagFloat(i, (float) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_DOUBLE:
//                        pstmt.setTagDouble(i, (double) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_SMALLINT:
//                        pstmt.setTagShort(i, (short) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_BINARY:
//                        pstmt.setTagString(i, (String) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_JSON:
//                        pstmt.setTagJson(i, (String) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_NCHAR:
//                        pstmt.setTagNString(i, (String) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_GEOMETRY:
//                        pstmt.setTagGeometry(i, (byte[]) tagParams.get(i - 1));
//                        break;
//                    case TaosType.TSDB_DATA_TYPE_VARBINARY:
//                        pstmt.setTagVarbinary(i, (byte[]) tagParams.get(i - 1));
//                        break;
//                    default:
//                        LOG.error("setStmtTag tag type is error, type:{}", metaInfo.getFieldType().getTypeName());
//                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE);
//                }
//            }
//        }
//    }

    private String getStringParam(Object columnParam, int taosType) throws SQLException, UnsupportedEncodingException {
        if (columnParam == null) {
            return "NULL";
        }

        switch (taosType) {
            case TaosType.TSDB_DATA_TYPE_BOOL:
            case TaosType.TSDB_DATA_TYPE_INT:
            case TaosType.TSDB_DATA_TYPE_TINYINT:
            case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                if (columnParam instanceof Timestamp) {
                    return "" + ((Timestamp) columnParam).getTime();
                }else if (columnParam instanceof String) {
                    return "'" + columnParam +"'";
                }
            case TaosType.TSDB_DATA_TYPE_BIGINT:
            case TaosType.TSDB_DATA_TYPE_FLOAT:
            case TaosType.TSDB_DATA_TYPE_DOUBLE:
            case TaosType.TSDB_DATA_TYPE_SMALLINT:
                return String.valueOf(columnParam);
            case TaosType.TSDB_DATA_TYPE_VARCHAR:
                StringBuilder value = new StringBuilder("'");
                if (columnParam instanceof byte[]) {
                    value.append(new String((byte[]) columnParam, "UTF-8"));
                } else {
                    value.append((String) columnParam);
                }
                value.append("'");
                return value.toString();
            case TaosType.TSDB_DATA_TYPE_NCHAR:
                return "'" + columnParam + "'";
            default:
                LOG.error("setStmtLineParams param type is error, type:{}", taosType);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
        }

    }

}
