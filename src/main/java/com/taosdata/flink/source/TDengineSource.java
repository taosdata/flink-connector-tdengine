package com.taosdata.flink.source;

import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.TDengineSourceRecordsWithSplitsIds;
import com.taosdata.flink.source.enumerator.TDengineSourceEnumerator;
import com.taosdata.flink.source.serializable.TDengineSourceEnumStateSerializer;
import com.taosdata.flink.source.enumerator.TDengineSourceEnumState;
import com.taosdata.flink.source.serializable.TDengineSplitSerializer;
import com.taosdata.flink.source.reader.TDengineRecordEmitter;
import com.taosdata.flink.source.reader.TDengineSourceReader;
import com.taosdata.flink.source.split.TDengineSplit;
import com.taosdata.flink.source.split.TDengineSplitReader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class TDengineSource<OUT> implements Source<OUT, TDengineSplit, TDengineSourceEnumState>, ResultTypeQueryable<OUT>{
    private final Logger LOG = LoggerFactory.getLogger(TDengineSource.class);
    private Properties properties;
    private SourceSplitSql sourceSql;
    private boolean isBatchMode = false;
    private final Class<OUT> typeClass;

    public TDengineSource(Properties properties, SourceSplitSql sql, Class<OUT> typeClass) {
        this.properties = properties;
        this.sourceSql = sql;
        this.typeClass = typeClass;
        String batchMode = this.properties.getProperty(TDengineConfigParams.TD_BATCH_MODE, "false");
        if (batchMode.equals("true")) {
            isBatchMode = true;
        }
        LOG.info("source properties:{}", this.properties.toString());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
    @Override
    public SourceReader<OUT, TDengineSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        // create TDengineSplitReader
        Supplier<TDengineSplitReader> splitReaderSupplier =
                ()-> {
                    try {
                        return new TDengineSplitReader<OUT>(this.properties, sourceReaderContext);
                    } catch (ClassNotFoundException e) {
                        LOG.error("create TDengineSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        LOG.error("create TDengineSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        LOG.error("create TDengineSplitReader exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    }

                };

        SingleThreadFetcherManager fetcherManager = new SingleThreadFetcherManager(splitReaderSupplier);

        RecordEmitter recordEmitter;
        if (isBatchMode) {
            recordEmitter = new TDengineRecordEmitter<OUT>(true);
        }else{
            recordEmitter = new TDengineRecordEmitter<OUT>(false);
        }

        return new TDengineSourceReader<>(fetcherManager, recordEmitter, toConfiguration(this.properties), sourceReaderContext);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TDengineSourceEnumState> createEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext) throws Exception {
        return new TDengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql);
    }

    @Override
    public SplitEnumerator<TDengineSplit, TDengineSourceEnumState> restoreEnumerator(SplitEnumeratorContext<TDengineSplit> splitEnumeratorContext, TDengineSourceEnumState splitsState) throws Exception {
        return new TDengineSourceEnumerator(splitEnumeratorContext, this.getBoundedness(), this.sourceSql, splitsState);
    }

    @Override
    public SimpleVersionedSerializer<TDengineSplit> getSplitSerializer() {
        return new TDengineSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<TDengineSourceEnumState> getEnumeratorCheckpointSerializer() {
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
