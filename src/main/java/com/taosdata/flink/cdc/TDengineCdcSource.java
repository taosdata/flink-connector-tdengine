package com.taosdata.flink.cdc;

import com.taosdata.flink.cdc.enumerator.TdengineCdcEnumState;
import com.taosdata.flink.cdc.enumerator.TdengineCdcEnumerator;
import com.taosdata.flink.cdc.reader.TDengineCdcEmitter;
import com.taosdata.flink.cdc.reader.TDengineCdcFetcherManager;
import com.taosdata.flink.cdc.reader.TDengineCdcReader;
import com.taosdata.flink.cdc.serializable.TDengineCdcEnumStateSerializer;
import com.taosdata.flink.cdc.serializable.TDengineCdcSplitSerializer;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TdengineCdcSplitReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
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

public class TDengineCdcSource<OUT> implements Source<OUT, TDengineCdcSplit, TdengineCdcEnumState>, ResultTypeQueryable<OUT>{
    private String topic;
    private Properties properties;

    Class<OUT> typeClass;
    public TDengineCdcSource(String topic, Properties properties, Class<OUT> typeClass) {
        this.topic = topic;
        this.properties = properties;
        this.typeClass = typeClass;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TDengineCdcSplit, TdengineCdcEnumState> createEnumerator(SplitEnumeratorContext<TDengineCdcSplit> enumContext) throws Exception {
        return new TdengineCdcEnumerator(enumContext, getBoundedness(), topic, properties);
    }
    @Override
    public SplitEnumerator<TDengineCdcSplit, TdengineCdcEnumState> restoreEnumerator(SplitEnumeratorContext<TDengineCdcSplit> enumContext, TdengineCdcEnumState checkpoint) throws Exception {
        return new TdengineCdcEnumerator(enumContext, getBoundedness(), topic, properties, checkpoint);
    }
    @Override
    public SourceReader<OUT, TDengineCdcSplit> createReader(SourceReaderContext readerContext) throws Exception {
        Supplier<TdengineCdcSplitReader> splitReaderSupplier =
                ()-> {
                    try {
                        return new TdengineCdcSplitReader<OUT>(this.topic, this.properties, readerContext);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                };

        FutureCompletingBlockingQueue<RecordsWithSplitIds<OUT>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        SingleThreadFetcherManager fetcherManager = new TDengineCdcFetcherManager(elementsQueue, splitReaderSupplier);
        String autoCommit = this.properties.getProperty("enable.auto.commit", "true");
        return new TDengineCdcReader<>(fetcherManager, new TDengineCdcEmitter<OUT>(), toConfiguration(this.properties), readerContext, autoCommit);

    }

    @Override
    public SimpleVersionedSerializer<TDengineCdcSplit> getSplitSerializer() {
        return new TDengineCdcSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TdengineCdcEnumState> getEnumeratorCheckpointSerializer() {
        return new TDengineCdcEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        String outType = this.properties.getProperty("value.deserializer");
        if (outType == "RowData") {
            return (TypeInformation<OUT>) TypeInformation.of(RowData.class);
        }else if(outType == "Map") {
            Map<String, Object> map = new HashMap<>();
            return (TypeInformation<OUT>) TypeInformation.of(map.getClass());
        }
        return TypeInformation.of(this.typeClass);
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }


}
