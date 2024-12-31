package com.taosdata.flink.source.serializable;

import com.taosdata.flink.source.entity.SplitResultRecord;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;
import java.sql.SQLException;

public interface TDengineRecordDeserialization<T> extends Serializable, ResultTypeQueryable<T> {
    /**
     * If the user needs to convert the data type in the data source to a custom data type, the implementation of this method needs to be provided. For specific implementation, please refer to {@link TDengineRowDataDeserialization}
     * @param splitResultRecord The input data type is object, and users can convert it based on the information in metaData.
     * @return user-defined data structure
     * @throws SQLException
     */
    public T convert(SplitResultRecord splitResultRecord) throws SQLException;
}
