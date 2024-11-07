package com.taosdata.flink.source;

import com.taosdata.flink.sink.TaosSinkConnector;
import com.taosdata.flink.source.entity.SourceError;
import com.taosdata.flink.source.entity.SourceErrorNumbers;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.tmq.TopicPartition;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import java.util.Properties;


public class TaosCdcSource <T>  extends RichParallelSourceFunction<ConsumerRecords<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(TaosSinkConnector.class);
    private Properties properties;
    private static volatile boolean running = true;
    private boolean isOutTypeRowData = false;
    private List<String> topics;

    private TaosConsumer<T> consumer;

    public TaosCdcSource(String topic, Properties properties) throws SQLException {
        if (topic.isEmpty()) {
            throw SourceError.createSQLException(SourceErrorNumbers.ERROR_TMQ_TOPIC);
        }
        this.topics  = Collections.singletonList(topic);
        this.properties = properties;
        this.properties.setProperty("td.connect.type", "ws");
        String outType = this.properties.getProperty("source.out.type");
        if (outType == "RowData") {
            isOutTypeRowData = true;
        }
        String url = this.properties.getProperty("bootstrap.servers");
        if (url.isEmpty()) {
            throw SourceError.createSQLException(SourceErrorNumbers.ERROR_SERVER_ADDRESS);
        }
        String groupId = this.properties.getProperty("group.id");
        if (url.isEmpty()) {
            throw SourceError.createSQLException(SourceErrorNumbers.ERROR_TMQ_GROUP_ID_CONFIGURATION);
        }
    }

    private  TaosConsumer<T> initConsumer() throws Exception {
        try {
            TaosConsumer<T> consumer= new TaosConsumer<T>(this.properties);
            return consumer;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Override
    public void run(SourceContext<ConsumerRecords<T>> sourceContext) throws Exception {
        this.consumer = initConsumer();

        try {
            // subscribe to the topics
            this.consumer.subscribe(topics);
            TopicPartition topicPartition = new TopicPartition(topics.get(0), 0);
            while (this.running) {
                // poll data
                if (this.isOutTypeRowData) {
                    ConsumerRecords<List<Object>> records = (ConsumerRecords<List<Object>>) consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        ConsumerRecords<RowData> result = new ConsumerRecords();
                        for (ConsumerRecord<List<Object>> record : records) {
                            List<Object> objectList= record.value();
                            RowData rowData = GenericRowData.of(objectList);
                            result.put(topicPartition, new ConsumerRecord(topics.get(0), "", 0, rowData));
                        }
                        sourceContext.collect((ConsumerRecords<T>) result);
                    }

                } else {
                    ConsumerRecords<T> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        sourceContext.collect(records);
                    }

                }
            }
            this.consumer.unsubscribe();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            // close the consumer
            this.consumer.close();
        }

    }

    @Override
    public void cancel() {
        if (running) {
            running = false;
        }

    }

}
