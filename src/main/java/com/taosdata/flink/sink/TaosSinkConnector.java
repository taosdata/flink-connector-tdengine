package com.taosdata.flink.sink;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.taosdata.flink.sink.entity.*;
import com.taosdata.jdbc.TSDBDriver;
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

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class TaosSinkConnector<T> extends RichSinkFunction<T> implements CheckpointListener, CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TaosSinkConnector.class);
    private Properties properties;
    private String url;
    private Connection conn;
    public TaosSinkConnector(String url, Properties properties) {
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        this.properties = properties;
        this.url = url;
        LOG.info("init connect websocket url:" + this.url);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.conn = DriverManager.getConnection(this.url, this.properties);
        } catch (SQLException e) {
            LOG.error("open exception url:" + this.url, e.getSQLState());
        }

        LOG.info("connect websocket url:" + this.url);
    }
    private void setStmtTag(TSWSPreparedStatement pstmt, List<TagParam> tagParams) throws Exception {
        if (tagParams != null && tagParams.size() > 0) {
            for (int i = 1; i <= tagParams.size(); i++) {
                TagParam tagParam = tagParams.get(i - 1);
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
                        LOG.error("setStmtTag tag type is error, type:{}", tagParam.getType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE);
                }
            }
        }
    }

    private void setStmtLineParams(TSWSPreparedStatement pstmt, List<List<TagParam>> params) throws Exception {
        for (List<TagParam> lineTagParams : params) {
            for (int i = 1; i <= lineTagParams.size(); i++) {
                TagParam tagParam = lineTagParams.get(i - 1);
                switch (tagParam.getType().getTypeNo()) {
                    case TaosType.TSDB_DATA_TYPE_BOOL:
                        pstmt.setBoolean(i, (boolean) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_INT:
                        pstmt.setInt(i, (int) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_TINYINT:
                        pstmt.setByte(i, (byte) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_TIMESTAMP:
                        pstmt.setTimestamp(i, new Timestamp((long) tagParam.getValue()));
                        break;
                    case TaosType.TSDB_DATA_TYPE_BIGINT:
                        pstmt.setLong(i, (long) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_FLOAT:
                        pstmt.setFloat(i, (float) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_DOUBLE:
                        pstmt.setDouble(i, (double) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_SMALLINT:
                        pstmt.setShort(i, (short) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_BINARY:
                        pstmt.setString(i, (String) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_NCHAR:
                        pstmt.setNString(i, (String) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_GEOMETRY:
                        pstmt.setGeometry(i, (byte[]) tagParam.getValue());
                        break;
                    case TaosType.TSDB_DATA_TYPE_VARBINARY:
                        pstmt.setVarbinary(i, (byte[]) tagParam.getValue());
                        break;
                    default:
                        LOG.error("setStmtLineParams param type is error, type:{}", tagParam.getType().getTypeName());
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
                }
            }
            pstmt.addBatch();
        }
    }

    private void setStmtParams(TSWSPreparedStatement pstmt, List<ColumnParam> params) throws Exception {
        for (int i = 1; i <= params.size(); i++) {
            ColumnParam param = params.get(i - 1);
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
                    LOG.error("setStmtParams param type is error, type:{}", param.getType().getTypeName());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
            }
        }
    }

    private String getSuperTableSql(SuperTableData data) {
        if (Strings.isNullOrEmpty(data.getDbName()) || data.getColumnNames() == null || data.getColumnNames().isEmpty()) {
            LOG.error("StatementData param error:{}", JSON.toJSONString(data));
            return "";
        }

        if (Strings.isNullOrEmpty(data.getSuperTableName()) || data.getTagNames() == null || data.getTagNames().isEmpty() ) {
            String sql = "INSERT INTO ? (" + String.join(",", data.getColumnNames()) + ") VALUES (?";
            for (int i = 1; i < data.getColumnNames().size(); i++) {
                sql += ",?";
            }
            sql += ")";
            return sql;
        }

        String sql = "INSERT INTO ? USING `" + data.getDbName() + "`.`" + data.getSuperTableName() + "` (";
        sql += String.join(",", data.getTagNames()) + ") TAGS (?";

        for (int i = 1; i < data.getTagNames().size(); i++) {
            sql += ",?";
        }

        sql += ") (" + String.join(",", data.getColumnNames()) + ") VALUES (?";
        for (int i = 1; i < data.getColumnNames().size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }

    private String getNormalTableSql(NormalTableData data) {
        if (Strings.isNullOrEmpty(data.getDbName()) || Strings.isNullOrEmpty(data.getTableName())
                ||data.getColumnNames() == null || data.getColumnNames().isEmpty()) {
            LOG.error("NormalTableData param error:{}", JSON.toJSONString(data));
            return "";
        }
        String sql = "INSERT INTO ? (" + String.join(",", data.getColumnNames()) + ") VALUES (?";
        for (int i = 1; i < data.getColumnNames().size(); i++) {
            sql += ",?";
        }
        sql += ")";
        return sql;
    }
    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value == null) {
            LOG.error("invoke value is null");
            return;
        }
        if (null == this.conn) {
            this.conn = DriverManager.getConnection(this.url, this.properties);
            LOG.info("invoke connect websocket url:" + this.url);
        }

        if (value instanceof SuperTableData) {
            SuperTableData superTableData = (SuperTableData)value;
            if (Strings.isNullOrEmpty(superTableData.getDbName())) {
                throw SinkError.createSQLException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
            }
            String sql = getSuperTableSql(superTableData);
            if (Strings.isNullOrEmpty(sql)) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
            }

            List<SubTableData> subTableDataList = superTableData.getSubTableDataList();
            if (subTableDataList == null || subTableDataList.isEmpty()) {
                LOG.error("invoke tableDataList is null");
                return;
            }
            for (SubTableData subTableData : subTableDataList) {
                try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
                    pstmt.setTableName(superTableData.getDbName() + "." + subTableData.getTableName());
                    setStmtTag(pstmt, subTableData.getTagParams());
                    setStmtParams(pstmt, subTableData.getColumParams());
                    pstmt.columnDataAddBatch();
                    pstmt.columnDataExecuteBatch();

                } catch (SQLException e) {
                    LOG.error("invoke exception sql:{}", sql, e.getSQLState());
                    throw e;
                }
            }

        } else if (value instanceof NormalTableData) {
            NormalTableData normalTableData = (NormalTableData) value;
            if (Strings.isNullOrEmpty(normalTableData.getDbName())) {
                throw SinkError.createSQLException(SinkErrorNumbers.ERROR_DB_NAME_NULL);
            }

            if (Strings.isNullOrEmpty(normalTableData.getTableName())) {
                throw SinkError.createSQLException(SinkErrorNumbers.ERROR_TABLE_NAME_NULL);
            }

            String sql = getNormalTableSql(normalTableData);
            if (Strings.isNullOrEmpty(sql)) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
            }
            List<ColumnParam> columnParams = normalTableData.getColumParams();
            if (columnParams == null || columnParams.isEmpty()) {
                LOG.error("invoke normalTableData columParams is null");
                return;
            }
            try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
                pstmt.setTableName(normalTableData.getDbName() + "." + normalTableData.getTableName());
                setStmtParams(pstmt, normalTableData.getColumParams());
                pstmt.columnDataAddBatch();
                pstmt.columnDataExecuteBatch();
            } catch (SQLException e) {
                LOG.error("invoke exception sql:{}", sql, e.getSQLState());
                throw e;
            }

        } else if (value instanceof SqlData) {
            SqlData sqlData = (SqlData) value;
            if (sqlData.getSqlList() == null || sqlData.getSqlList().isEmpty()) {
                LOG.error("invoke sqlList is null");
                return;
            }
            try (Statement statement = this.conn.createStatement()) {
                if (!Strings.isNullOrEmpty(sqlData.getDbName())) {
                    statement.executeUpdate("USE " + sqlData.getDbName());
                }

                for (String sql : sqlData.getSqlList()) {
                    statement.executeUpdate(sql);
                }
            } catch (SQLException e) {
                LOG.error("invoke sql exception {}", e.getSQLState());
                throw e;
            }
        } else {
            LOG.error("invoke input params data type wrong:{}", JSON.toJSONString(value));
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
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
