package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class ActiveMqSinkTask extends SinkTask {

    private JmsProducer producer;

    @Override
    public String version() {
        return AppVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        producer = createProducer();
    }

    JmsProducer createProducer() {
        return new ActiveMqProducer();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        collection.forEach(
                r -> producer.write(r.value().toString())
        );
    }

    @Override
    public void stop() {

    }
}
