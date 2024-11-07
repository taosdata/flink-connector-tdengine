package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceSplitSql;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;
import java.util.function.Supplier;

public class TdengineSource<OUT> implements Source<OUT, TdengineSplit, TdengineSourceEnumState>, ResultTypeQueryable<OUT>{
    private String url;
    private Properties properties;
    private SourceSplitSql sourceSql;
    private TdengineRecordDeserialization<OUT> tdengineRecordDeserialization;
    public TdengineSource(String url, Properties properties, SourceSplitSql sql, TdengineRecordDeserialization<OUT> tdengineRecordDeserialization) {
        this.url = url;
        this.properties = properties;
        this.sourceSql = sql;
        this.tdengineRecordDeserialization = tdengineRecordDeserialization;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
    @Override
    public SourceReader<OUT, TdengineSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        Supplier<TdengineSplitReader> splitFetcherManager =
                ()-> {

                        String sql = "select " + this.sourceSql.getSelect()
                                + " from `" + this.sourceSql.getTableName()
                                + "` where " + this.sourceSql.getWhere();
                    try {
                        return new TdengineSplitReader(this.url, this.properties, sql, sourceReaderContext);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                };
        SingleThreadFetcherManager fetcherManager = new SingleThreadFetcherManager(splitFetcherManager);

        return new TdengineSourceReader<>(fetcherManager, new TdengineRecordEmitter<OUT>(this.tdengineRecordDeserialization), toConfiguration(this.properties), sourceReaderContext);
    }

    @Override
    public SplitEnumerator<TdengineSplit, TdengineSourceEnumState> createEnumerator(SplitEnumeratorContext<TdengineSplit> splitEnumeratorContext) throws Exception {
        return new TdengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql);
    }

    @Override
    public SplitEnumerator<TdengineSplit, TdengineSourceEnumState> restoreEnumerator(SplitEnumeratorContext<TdengineSplit> splitEnumeratorContext, TdengineSourceEnumState TdengineSplitsState) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<TdengineSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<TdengineSourceEnumState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.tdengineRecordDeserialization.getProducedType();
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
