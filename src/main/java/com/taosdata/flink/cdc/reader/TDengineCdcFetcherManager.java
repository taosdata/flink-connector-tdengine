package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitReader;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

public class TDengineCdcFetcherManager<T>  extends SingleThreadFetcherManager<T, TDengineCdcSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineCdcFetcherManager.class);

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param splitReaderSupplier The factory for the split reader that connects to the source system.
     */
    public TDengineCdcFetcherManager(Supplier<SplitReader<T, TDengineCdcSplit>> splitReaderSupplier, Configuration configuration) {
        super(splitReaderSupplier, configuration);
    }

    /**
     *  When the checkpoint is completed, call the commitOffsets of splitReader for data commit
     * @param offsetsToCommit The offset of data submission for vgroup
     */
    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) throws SQLException {
        LOG.debug("Committing offsets {}", offsetsToCommit);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        SplitFetcher<T, TDengineCdcSplit> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            TDengineCdcSplitReader splitReader = (TDengineCdcSplitReader) splitFetcher.getSplitReader();
            splitReader.commitOffsets(offsetsToCommit);
        }
    }
}

