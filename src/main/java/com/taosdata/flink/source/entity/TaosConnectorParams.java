package com.taosdata.flink.source.entity;

import com.taosdata.jdbc.TSDBDriver;

public class TaosConnectorParams extends TSDBDriver {
    public static final String PULL_INTERVAL = "pull.interval";
    public static final String BATCH_SIZE = "batch.size";
}
