package com.taosdata.flink.cdc.reader;

import com.taosdata.flink.cdc.split.TDengineCdcSplit;
import com.taosdata.flink.cdc.split.TDengineCdcSplitReader;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Fetcher manager of the CDC source, compatible with Flink 1.15 through 1.20+.
 *
 * <p>It deliberately calls the {@code (elementsQueue, splitReaderSupplier)} constructor:
 * it is the only {@link SingleThreadFetcherManager} constructor that exists on every
 * Flink version since 1.15, so a jar compiled against a newer Flink still runs on 1.15.
 * The {@code (Supplier, Configuration)} constructors were only introduced in Flink 1.19.
 */
public class TDengineCdcFetcherManager<T> extends SingleThreadFetcherManager<T, TDengineCdcSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(TDengineCdcFetcherManager.class);

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue handing over data from the fetcher to the reader.
     *                      It must be the same instance that is passed to the source reader.
     * @param splitReaderSupplier The factory for the split reader that connects to the source system.
     */
    @SuppressWarnings("deprecation")
    public TDengineCdcFetcherManager(FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
                                     Supplier<SplitReader<T, TDengineCdcSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
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
        // On Flink 1.15 ~ 1.20+ fetchers is a Map<Integer, SplitFetcher> keyed by fetcher id,
        // and the single fetcher created by SingleThreadFetcherManager is registered under id 0.
        SplitFetcher<T, TDengineCdcSplit> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            TDengineCdcSplitReader splitReader = (TDengineCdcSplitReader) splitFetcher.getSplitReader();
            splitReader.commitOffsets(offsetsToCommit);
        }
    }
}
