package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.entity.SplitResultRecord;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;
import java.sql.SQLException;

public interface TDengineRecordDeserialization<T> extends Serializable, ResultTypeQueryable<T> {
    public T convert(SplitResultRecord splitResultRecord) throws SQLException;
}
