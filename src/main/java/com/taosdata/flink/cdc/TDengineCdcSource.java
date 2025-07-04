package com.taosdata.flink.cdc;

import com.taosdata.flink.cdc.enumerator.TDengineCdcEnumState;
import com.taosdata.flink.cdc.enumerator.TDengineCdcEnumerator;
import com.taosdata.flink.cdc.reader.TDengineCdcEmitter;
import com.taosdata.flink.cdc.reader.TDengineCdcFetcherManager;
import com.taosdata.flink.cdc.reader.TDengineCdcReader;
import com.taosdata.flink.cdc.serializable.TDengineCdcEnumStateSerializer;
import com.taosdata.flink.cdc.serializable.TDengineCdcSplitSerializer;
import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitReader;
import com.taosdata.flink.common.TDengineCdcParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class TDengineCdcSource<OUT> implements Source<OUT, TDengineCdcSplit, TDengineCdcEnumState>, ResultTypeQueryable<OUT>{
    private final Logger LOG = LoggerFactory.getLogger(TDengineCdcSource.class);
    private String topic;
    private Properties properties;

    private boolean isBatchMode = false;

    private boolean isAutoCommit = false;

    private final Class<OUT> typeClass;
    public TDengineCdcSource(String topic, Properties properties, Class<OUT> typeClass) {
        this.topic = topic;
        this.properties = properties;
        this.typeClass = typeClass;
        this.properties.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        String batchMode = this.properties.getProperty(TDengineCdcParams.TMQ_BATCH_MODE, "false");
        if (batchMode.equals("true")) {
            isBatchMode = true;
        }

        this.properties.remove(TDengineCdcParams.TMQ_BATCH_MODE);

        String autoCommit = this.properties.getProperty(TDengineCdcParams.ENABLE_AUTO_COMMIT, "false");
        if (autoCommit.equals("true")) {
            isAutoCommit = true;
        } else {
            this.properties.setProperty(TDengineCdcParams.ENABLE_AUTO_COMMIT, "false");
        }
        LOG.info("cdc properties:{}", this.properties.toString());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TDengineCdcSplit, TDengineCdcEnumState> createEnumerator(SplitEnumeratorContext<TDengineCdcSplit> enumContext) throws Exception {
        return new TDengineCdcEnumerator(enumContext, getBoundedness(), topic, properties);
    }
    @Override
    public SplitEnumerator<TDengineCdcSplit, TDengineCdcEnumState> restoreEnumerator(SplitEnumeratorContext<TDengineCdcSplit> enumContext, TDengineCdcEnumState checkpoint) throws Exception {
        return new TDengineCdcEnumerator(enumContext, getBoundedness(), topic, properties, checkpoint);
    }
    @Override
    public SourceReader<OUT, TDengineCdcSplit> createReader(SourceReaderContext readerContext) throws Exception {
        // create TDengineCdcSplitReader
        Supplier<TDengineCdcSplitReader> splitReaderSupplier =
                ()-> {
                    try {
                        return new TDengineCdcSplitReader<OUT>(this.topic, this.properties, readerContext);
                    } catch (ClassNotFoundException e) {
                        LOG.error("create TDengineCdcSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        LOG.error("create TDengineCdcSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        LOG.error("create TDengineCdcSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    }

                };
        Configuration configuration = new Configuration();
        configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1000);
        SingleThreadFetcherManager fetcherManager = new TDengineCdcFetcherManager(splitReaderSupplier, configuration);

        RecordEmitter recordEmitter;
        if (isBatchMode) {
            recordEmitter = new TDengineCdcEmitter<OUT>(true);
        }else{
            recordEmitter = new TDengineCdcEmitter<OUT>(false);
        }

        return new TDengineCdcReader<>(fetcherManager, recordEmitter, toConfiguration(this.properties), readerContext, isAutoCommit);

    }

    @Override
    public SimpleVersionedSerializer<TDengineCdcSplit> getSplitSerializer() {
        return new TDengineCdcSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TDengineCdcEnumState> getEnumeratorCheckpointSerializer() {
        return new TDengineCdcEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        String outType = this.properties.getProperty("value.deserializer");
        if (!isBatchMode) {
            if (outType.equals("RowData")) {
                return (TypeInformation<OUT>) TypeInformation.of(RowData.class);
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
