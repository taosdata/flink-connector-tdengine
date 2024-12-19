package com.taosdata.flink.cdc.split;

import com.google.common.base.Strings;
import com.taosdata.flink.cdc.entity.CdcRecords;
import com.taosdata.flink.cdc.entity.CdcTopicPartition;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.OffsetAndMetadata;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;

public class TDengineCdcSplitReader<OUT> implements SplitReader<CdcRecords<OUT>, TDengineCdcSplit> {
    private final Logger LOG = LoggerFactory.getLogger(TDengineCdcSplitReader.class);
    private Properties properties;
    private String topic;
    private int subtaskId;
    private List<TDengineCdcSplit> tdengineSplits;
    private List<TDengineCdcSplit> finishedSplits;
    private int pollIntervalMs = 100;
    private String groupId;
    private String clientId;
    private String splitId;
    private TaosConsumer<OUT> consumer;

    public TDengineCdcSplitReader(String topic, Properties properties, SourceReaderContext context) throws ClassNotFoundException, SQLException {
        this.subtaskId = context.getIndexOfSubtask();
        this.topic = topic;
        this.finishedSplits = new ArrayList<>();
        this.tdengineSplits = new ArrayList<>();
        this.properties = properties;
        this.properties.setProperty("td.connect.type", "ws");
        String pollInterval = this.properties.getProperty("poll.interval.ms");
        if (!Strings.isNullOrEmpty(pollInterval)) {
            pollIntervalMs = Integer.parseInt(pollInterval);
        }
        String outType = this.properties.getProperty("value.deserializer");
        if (outType.equals("RowData")) {
            this.properties.setProperty("value.deserializer", "com.taosdata.flink.cdc.serializable.RowDataCdcDeserializer");
        }else if(outType.equals("Map")) {
            this.properties.setProperty("value.deserializer", "");
        }
    }

    private void creatConsumer() throws SQLException {
        try {
            this.consumer = new TaosConsumer<>(this.properties);
            consumer.subscribe(Collections.singletonList(topic));

            //todo test
            consumer.seekToBeginning(consumer.assignment());
        } catch (SQLException ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create websocket consumer, host: %s, groupId: %s, clientId: %s, %sErrMessage: %s%n",
                    properties.getProperty("bootstrap.servers"),
                    properties.getProperty("group.id"),
                    properties.getProperty("client.id"),
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    @Override
    public RecordsWithSplitIds<CdcRecords<OUT>> fetch() throws IOException {
        try {
            if (this.consumer == null) {
                creatConsumer();
            }

            List<CdcTopicPartition> topicPartitions = new ArrayList<>();
            ConsumerRecords<OUT> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
            if (!records.isEmpty()) {
                for (TopicPartition tp : consumer.assignment()) {
                    long position = consumer.position(tp);
                    CdcTopicPartition cdcTopicPartition = new CdcTopicPartition(tp.getTopic(), position, tp.getVGroupId());
                    topicPartitions.add(cdcTopicPartition);
                }
            }

            if (!records.isEmpty()) {
                LOG.debug("Succeed to poll data, topic: %s, groupId: %s, clientId: %s, %sErrMessage: %s%n",
                        topic, groupId, clientId);
            }
            return new TDengineRecordsWithSplitIds(splitId, records, topicPartitions);

        } catch (SQLException ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            LOG.error("Failed to poll data, topic: %s, groupId: %s, clientId: %s, %sErrMessage: %s%n",
                    topic,
                    groupId,
                    clientId,
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw new IOException(ex.getMessage());
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TDengineCdcSplit> splitsChange) {
        List<TDengineCdcSplit> splits = splitsChange.splits();
        this.tdengineSplits.addAll(splits);
        this.groupId = splits.get(0).getGroupId();
        this.clientId = splits.get(0).getClientId();
        this.properties.setProperty("group.id", splits.get(0).getGroupId());
        this.properties.setProperty("client.id", splits.get(0).getClientId());
        this.splitId = splits.get(0).splitId();
    }

    @Override
    public void wakeUp() {

    }
    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) throws SQLException {
        if (offsetsToCommit != null && !offsetsToCommit.isEmpty()) {
            this.consumer.commitSync(offsetsToCommit);
        }
    }
    @Override
    public void close() throws Exception {
        if (this.consumer != null) {
            this.consumer.unsubscribe();
            this.consumer.close();
        }
    }

}
