package com.taosdata.flink.sink.entity;

/**
 * 系统增加新的无符号数据类型，分别是：
 * unsigned tinyint， 数值范围：0-254, NULL 为255
 * unsigned smallint，数值范围： 0-65534， NULL 为65535
 * unsigned int，数值范围：0-4294967294，NULL 为4294967295u
 * unsigned bigint，数值范围：0-18446744073709551614u，NULL 为18446744073709551615u。
 * example:
 * create table tb(ts timestamp, a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned)),
 */
public enum DataType {
    DATA_TYPE_BOOL("BOOL", 1),
    DATA_TYPE_TINYINT("TINYINT", 2),
    DATA_TYPE_SMALLINT("SMALLINT", 3),
    DATA_TYPE_INT("INT", 4),
    DATA_TYPE_BIGINT("BIGINT", 5),
    DATA_TYPE_FLOAT("FLOAT", 6),
    DATA_TYPE_DOUBLE("DOUBLE", 7),
    DATA_TYPE_VARCHAR("VARCHAR", 8),
    DATA_TYPE_BINARY("BINARY", 8),
    DATA_TYPE_TIMESTAMP("TIMESTAMP", 9),
    DATA_TYPE_NCHAR("NCHAR", 10),

    DATA_TYPE_JSON("JSON", 15),
    DATA_TYPE_VARBINARY("VARBINARY", 16),
    DATA_TYPE_GEOMETRY("GEOMETRY", 20),
    ;
    private final String typeName;
    private final int typeNo;

    DataType(String typeName, int typeNo) {
        this.typeName = typeName;
        this.typeNo = typeNo;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getTypeNo() {
        return typeNo;
    }


}
