package com.taosdata.flink.sink;

import java.io.Serializable;

public class TaosOptions implements Serializable {
    private final String userName;
    private final String pwd;
    private final String host;
    private final int port;
    public TaosOptions(String userName, String pwd, String host, int port) {
        this.userName = userName;
        this.pwd = pwd;
        this.host = host;
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public String getPwd() {
        return pwd;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
