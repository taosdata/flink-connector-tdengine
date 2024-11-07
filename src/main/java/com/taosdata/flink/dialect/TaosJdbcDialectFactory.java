//package com.taosdata.flink.dialect;
//
//import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
//import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;
//
//public class TaosJdbcDialectFactory implements JdbcDialectFactory {
//
//    public boolean acceptsURL(String url) {
//        return url.startsWith("jdbc:TAOS-RS:");
//    }
//    @Override
//    public JdbcDialect create() {
//        return new TaosJdbcDialect();
//    }
//
//}