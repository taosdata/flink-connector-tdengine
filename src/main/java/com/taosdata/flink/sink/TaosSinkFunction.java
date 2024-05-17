package com.taosdata.flink.sink;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class TaosSinkFunction<T> extends RichSinkFunction<T> implements CheckpointListener, CheckpointedFunction {
    private static final Logger logger = LoggerFactory.getLogger(TaosSinkFunction.class);
    private TaosOptions taosOptions;
    private Connection conn;
    public TaosSinkFunction(TaosOptions taosOptions) {
        this.taosOptions = taosOptions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String jdbcUrl = "jdbc:TAOS-RS://" + this.taosOptions.getHost() + ":"+ this.taosOptions.getPort() +"/?batchfetch=true";
        this.conn = DriverManager.getConnection(jdbcUrl, this.taosOptions.getUserName(), this.taosOptions.getPwd());
        logger.info("connect websocket url:", jdbcUrl);
    }
    private void setStmtTag(TSWSPreparedStatement pstmt, List<Param> params) throws Exception {
        for (int i = 1; i <= params.size(); i++) {
            Param tagParam = params.get(i - 1);
            switch (tagParam.getType().getTypeNo()) {
                case TaosType.TSDB_DATA_TYPE_BOOL:
                    pstmt.setTagBoolean(i, (boolean) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_INT:
                    pstmt.setTagInt(i, (int) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_TINYINT:
                    pstmt.setTagByte(i, (byte) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                    pstmt.setTagTimestamp(i, (long) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_BIGINT:
                    pstmt.setTagLong(i, (long) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_FLOAT:
                    pstmt.setTagFloat(i, (float) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_DOUBLE:
                    pstmt.setTagDouble(i, (double) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_SMALLINT:
                    pstmt.setTagShort(i, (short) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_BINARY:
                    pstmt.setTagString(i, (String) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_JSON:
                    pstmt.setTagJson(i, (String) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_NCHAR:
                    pstmt.setTagNString(i, (String) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                    pstmt.setTagGeometry(i, (byte[]) tagParam.getValue());
                    break;
                case TaosType.TSDB_DATA_TYPE_VARBINARY:
                    pstmt.setTagVarbinary(i, (byte[]) tagParam.getValue());
                    break;
                default:
                    logger.error("setStmtTag tag type is error, type:", tagParam.getType().getTypeName());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE);
            }
        }

    }

    private void setStmtLineParams(TSWSPreparedStatement pstmt, List<List<Param>> params) throws Exception {
        for (List<Param> lineParams : params) {
            for (int i = 1; i <= lineParams.size(); i++) {
                Param param = lineParams.get(i - 1);
                switch (param.getType().getTypeNo()) {
                    case TaosType.TSDB_DATA_TYPE_BOOL:
                        pstmt.setBoolean(i, (boolean) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_INT:
                        pstmt.setInt(i, (int) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_TINYINT:
                        pstmt.setByte(i, (byte) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                        pstmt.setTimestamp(i, new Timestamp((long) param.getValue()));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BIGINT:
                        pstmt.setLong(i, (long) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_FLOAT:
                        pstmt.setFloat(i, (float) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_DOUBLE:
                        pstmt.setDouble(i, (double) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_SMALLINT:
                        pstmt.setShort(i, (short) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_BINARY:
                        pstmt.setString(i, (String) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_NCHAR:
                        pstmt.setNString(i, (String) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                        pstmt.setGeometry(i, (byte[]) param.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_VARBINARY:
                        pstmt.setVarbinary(i, (byte[]) param.getValue());
                        break;
                    default:
                        logger.error("setStmtLineParams param type is error, type:", param.getType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
                }
            }
            pstmt.addBatch();
        }
    }

    private void setStmtParams(TSWSPreparedStatement pstmt, List<StatementParam> params) throws Exception {
        for (int i = 1; i <= params.size(); i++) {
            StatementParam param = params.get(i - 1);
            switch (param.getType().getTypeNo()) {
                case TaosType.TSDB_DATA_TYPE_BOOL:
                    pstmt.setBoolean(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_INT:
                    pstmt.setInt(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_TINYINT:
                    pstmt.setByte(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                    pstmt.setTimestamp(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_BIGINT:
                    pstmt.setLong(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_FLOAT:
                    pstmt.setFloat(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_DOUBLE:
                    pstmt.setDouble(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_SMALLINT:
                    pstmt.setShort(i, param.getValues());
                    break;
                case TaosType.TSDB_DATA_TYPE_BINARY:
                    pstmt.setString(i, param.getValues(), param.getValues().size());
                    break;
                case TaosType.TSDB_DATA_TYPE_NCHAR:
                    pstmt.setNString(i, param.getValues(), param.getValues().size());
                    break;
                case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                    pstmt.setGeometry(i, param.getValues(), param.getValues().size());
                    break;
                case TaosType.TSDB_DATA_TYPE_VARBINARY:
                    pstmt.setVarbinary(i, param.getValues(), param.getValues().size());
                    break;
                default:
                    logger.error("setStmtParams param type is error, type:", param.getType().getTypeName());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
            }
        }
    }

    private String getStmtSql(StatementData data) {
        if (Strings.isNullOrEmpty(data.getDbName()) || Strings.isNullOrEmpty(data.getSupperTableName())
                || Strings.isNullOrEmpty(data.getTableName()) || data.getTagFieldNames() == null
                || data.getTagFieldNames().isEmpty() || data.getFieldNames() == null || data.getFieldNames().isEmpty()) {
            logger.error("StatementData param error:", JSON.toJSONString(data));
            return "";
        }
        String sql = "INSERT INTO ? USING " + data.getDbName() + "." + data.getSupperTableName() + " (";
        sql += String.join(",", data.getTagFieldNames()) + ") TAGS (?";

        for (int i = 1; i < data.getTagFieldNames().size(); i++) {
            sql += ",?";
        }

        sql += ") (" + String.join(",", data.getFieldNames()) + ") VALUES (?";
        for (int i = 1; i < data.getFieldNames().size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (null == this.conn) {
            String jdbcUrl = "jdbc:TAOS-RS://" + this.taosOptions.getHost() + ":" + this.taosOptions.getPort() +"/?batchfetch=true";
            this.conn = DriverManager.getConnection(jdbcUrl, this.taosOptions.getUserName(), this.taosOptions.getPwd());
        }

        if (value instanceof StatementData) {
            StatementData data = (StatementData)value;
            String sql = getStmtSql(data);
            if (Strings.isNullOrEmpty(sql)) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
            }

            try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
                pstmt.setTableName(data.getDbName() + "." +data.getTableName());
                setStmtTag(pstmt, data.getTagParams());
                if (data.getMode() == 1) {
                    setStmtParams(pstmt, data.getColumParams());
                    pstmt.columnDataAddBatch();
                    pstmt.columnDataExecuteBatch();
                }else {
                    setStmtLineParams(pstmt, data.getLineParams());
                    pstmt.executeBatch();
                }

            } catch (SQLException e) {
                logger.error("invoke exception sql:", sql, e.getSQLState());
                throw e;
            }
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        super.writeWatermark(watermark);
    }

    @Override
    public void finish() throws Exception {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        super.finish();
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        if (conn != null) {
            conn.close();
            conn = null;
        }


    }
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
