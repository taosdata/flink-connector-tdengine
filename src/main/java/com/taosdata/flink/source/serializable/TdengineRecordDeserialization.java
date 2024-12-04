package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.entity.SourceRecord;
import com.taosdata.flink.source.entity.SourceRecords;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public interface TdengineRecordDeserialization<T> extends Serializable, ResultTypeQueryable<T> {
    public T convert(SourceRecord sourceRecord) throws SQLException;
}
