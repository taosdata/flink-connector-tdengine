package com.taosdata.flink.source;

import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.TdengineSourceRecords;
import com.taosdata.flink.source.enumerator.TdengineSourceEnumerator;
import com.taosdata.flink.source.serializable.TDengineSourceEnumStateSerializer;
import com.taosdata.flink.source.enumerator.TdengineSourceEnumState;
import com.taosdata.flink.source.serializable.TDengineSplitSerializer;
import com.taosdata.flink.source.reader.TdengineRecordEmitter;
import com.taosdata.flink.source.reader.TdengineSourceReader;
import com.taosdata.flink.source.serializable.TdengineRecordDeserialization;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.split.TdengineSplitReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

public class TdengineSource<OUT> implements Source<OUT, TDengineSplit, TdengineSourceEnumState>, ResultTypeQueryable<OUT>{
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
    public SourceReader<OUT, TDengineSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        Supplier<TdengineSplitReader> splitReaderSupplier =
                ()-> {
                    try {
                        return new TdengineSplitReader(this.url, this.properties, sourceReaderContext);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                };

        FutureCompletingBlockingQueue<RecordsWithSplitIds<TdengineSourceRecords>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        SingleThreadFetcherManager fetcherManager = new SingleThreadFetcherManager(elementsQueue, splitReaderSupplier);

        return new TdengineSourceReader<>(fetcherManager, new TdengineRecordEmitter<OUT>(this.tdengineRecordDeserialization), toConfiguration(this.properties), sourceReaderContext);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TdengineSourceEnumState> createEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext) throws Exception {
        return new TdengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TdengineSourceEnumState> restoreEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext, TdengineSourceEnumState TdengineSplitsState) throws Exception {
        return new TdengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql);
    }

    @Override
    public SimpleVersionedSerializer<TDengineSplit> getSplitSerializer() {
        return new TDengineSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TdengineSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new TDengineSourceEnumStateSerializer();
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
