package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class ActiveMqSinkTask extends SinkTask {

    private JmsProducer producer;
    private static final String KEY_ACTIVE_MQ_JMX_ENDPOINT = "activemq.endpoint";
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq.queue";

    @Override
    public String version() {
        return AppVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

        producer = createProducer(
                map.get(KEY_ACTIVE_MQ_JMX_ENDPOINT),
                map.get(KEY_ACTIVE_MQ_QUEUE_NAME));

        producer.start();
    }

    JmsProducer createProducer(String activeMqEndpoint, String activeMqQueueName) {
        return new ActiveMqProducer(activeMqEndpoint, activeMqQueueName);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        collection.forEach(
                r -> producer.write(r.value().toString())
        );
    }

    @Override
    public void stop() {
        producer.stop();
    }
}
