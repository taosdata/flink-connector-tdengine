package com.taosdata.flink.common;

import com.taosdata.jdbc.TSDBDriver;

public class TDengineConfigParams extends TSDBDriver {

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String TD_SOURCE_TYPE = "td.source.type";

    public static final String TD_BATCH_MODE = "td.batch.mode";

    public static final String TD_JDBC_URL = "url";


    public static final String TD_SUPERTABLE_NAME = "super_table_name";

    public static final String TD_TABLE_NAME = "table_name";
    public static final String TD_BATCH_SIZE = "batch.size";

    public static final String TD_STMT2_VERSION = "stmt2.version";
}
