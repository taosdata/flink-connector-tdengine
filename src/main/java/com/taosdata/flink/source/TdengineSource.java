package com.taosdata.flink.source;

import com.taosdata.flink.cdc.reader.TDengineCdcEmitter;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.TDengineSourceRecordsWithSplitsIds;
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
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class TdengineSource<OUT> implements Source<OUT, TDengineSplit, TdengineSourceEnumState>, ResultTypeQueryable<OUT>{
    private String url;
    private Properties properties;
    private SourceSplitSql sourceSql;
    private boolean isBatchMode = false;
    private final Class<OUT> typeClass;

    public TdengineSource(String url, Properties properties, SourceSplitSql sql, Class<OUT> typeClass) {
        this.url = url;
        this.properties = properties;
        this.sourceSql = sql;
        this.typeClass = typeClass;
        String batchMode = this.properties.getProperty("td.batch.mode", "false");
        if (batchMode.equals("true")) {
            isBatchMode = true;
        }
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
                        return new TdengineSplitReader<OUT>(this.url, this.properties, sourceReaderContext);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                };

        FutureCompletingBlockingQueue<RecordsWithSplitIds<TDengineSourceRecordsWithSplitsIds>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        SingleThreadFetcherManager fetcherManager = new SingleThreadFetcherManager(elementsQueue, splitReaderSupplier);

        RecordEmitter recordEmitter;
        if (isBatchMode) {
            recordEmitter = new TdengineRecordEmitter<OUT>(true);
        }else{
            recordEmitter = new TdengineRecordEmitter<OUT>(false);
        }

        return new TdengineSourceReader<>(fetcherManager, recordEmitter, toConfiguration(this.properties), sourceReaderContext);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TdengineSourceEnumState> createEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext) throws Exception {
        return new TdengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TdengineSourceEnumState> restoreEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext, TdengineSourceEnumState splitsState) throws Exception {
        //todo restore
        return new TdengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql, splitsState);
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
        String outType = this.properties.getProperty("value.deserializer");
        if (!isBatchMode) {
            if (outType == "RowData") {
                return (TypeInformation<OUT>) TypeInformation.of(RowData.class);
            } else if (outType == "Map") {
                Map<String, Object> map = new HashMap<>();
                return (TypeInformation<OUT>) TypeInformation.of(map.getClass());
            }
        }
        return TypeInformation.of(this.typeClass);
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
