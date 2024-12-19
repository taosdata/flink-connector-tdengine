package com.taosdata.flink.common;

import com.taosdata.jdbc.TSDBDriver;

public class TDengineConnectorParams extends TSDBDriver {
    public static final String BATCH_SIZE = "batch.size";

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String TD_BATCH_MODE = "td.batch.mode";
}
