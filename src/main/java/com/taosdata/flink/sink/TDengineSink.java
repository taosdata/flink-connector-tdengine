package com.taosdata.flink.sink;

import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TDengineSink <IN> implements Sink<IN> {
    private final String dbName;
    private final String superTableName;

    private final String normalTableName;

    private final String url;

    private final Properties properties;

    private final TDengineSinkRecordSerializer<IN> serializer;

    private final List<SinkMetaInfo> metaInfos;

    public TDengineSink(String dbName, String superTableName, String normalTableName, String url, Properties properties, TDengineSinkRecordSerializer<IN> serializer, List<SinkMetaInfo> metaInfos) {
        this.dbName = dbName;
        this.superTableName = superTableName;
        this.normalTableName = normalTableName;
        this.url = url;
        this.properties = properties;
        this.serializer = serializer;
        this.metaInfos = metaInfos;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        try {
            List<SinkMetaInfo> tagMetaInfos = new ArrayList<>();
            List<SinkMetaInfo> columnMetaInfos = new ArrayList<>();
            if (metaInfos != null) {
                for (SinkMetaInfo metaInfo: metaInfos) {
                    if (metaInfo.isTag()) {
                        tagMetaInfos.add(metaInfo);
                    }else{
                        columnMetaInfos.add(metaInfo);
                    }
                }
                return new TDengineWriter<IN>(this.url, this.dbName, this.superTableName, this.normalTableName, this.properties, serializer, tagMetaInfos, columnMetaInfos);
            }
            throw new SQLException("meta info is null!");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
