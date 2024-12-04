package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;

import com.taosdata.flink.cdc.split.TdengineCdcSplitReader;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TDengineCdcFetcherManager<T>  extends SingleThreadFetcherManager<T, TDengineCdcSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(TDengineCdcFetcherManager.class);

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     */
    public TDengineCdcFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
                                     Supplier<SplitReader<T, TDengineCdcSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }
    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) throws SQLException {
        LOG.debug("Committing offsets {}", offsetsToCommit);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        SplitFetcher<T, TDengineCdcSplit> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            TdengineCdcSplitReader splitReader = (TdengineCdcSplitReader) splitFetcher.getSplitReader();
            splitReader.commitOffsets(offsetsToCommit);
        }
    }
}

