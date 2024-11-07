//package com.taosdata.flink.dialect;
//
//import org.apache.flink.annotation.Internal;
//import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
//import org.apache.flink.table.types.logical.RowType;
//
//@Internal
//public class TaosJdbcRowConverter extends AbstractJdbcRowConverter {
//    private static final long serialVersionUID = 1L;
//
//    public String converterName() {
//        return "Taos";
//    }
//
//    public TaosJdbcRowConverter(RowType rowType) {
//        super(rowType);
//    }
//}