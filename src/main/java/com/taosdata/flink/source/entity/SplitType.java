package com.taosdata.flink.source.entity;

public enum SplitType {
    SPLIT_TYPE_TIMESTAMP("TIMESTAMP", 1),
    SPLIT_TYPE_TABLE("TABLE", 2),

    SPLIT_TYPE_TAG("TAG", 3),
    SPLIT_TYPE_SQL("SQL", 4),
    ;

    private final String typeName;
    private final int typeNo;
    SplitType(String typeName, int typeNo) {
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
