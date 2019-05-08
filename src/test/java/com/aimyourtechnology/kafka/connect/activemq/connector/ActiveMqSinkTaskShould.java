package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ActiveMqSinkTaskShould {
    private static final String KAFKA_TOPIC = "aKafkaTopic";
    private static final int PARTITION = 42;
    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final String KEY = "KafkaMessageKey";
    private static final String KEY_ACTIVE_MQ_JMX_ENDPOINT = "activemq.endpoint";
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq.queue";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "<some><xml><perhaps/></xml></some>";
    private static final long OFFSET = 666L;
    private static final String ACTIVE_MQ_ENDPOINT = "endpoint";
    private static final String ACTIVE_MQ_QUEUE_NAME = "queue name";

    @Mock
    private JmsProducer activeMqProducer;

    @Spy
    private ActiveMqSinkTask task = new ActiveMqSinkTask();

    @Test
    public void haveVersion() {

        assertThat(task.version()).isNotEmpty();
    }

    @Test
    public void writeKafkaMessageToActiveMq() {
        List<SinkRecord> sinkRecords = new ArrayList<>();
        SinkRecord sinkRecord = createSinkRecord();
        sinkRecords.add(sinkRecord);

        when(task.createProducer(ACTIVE_MQ_ENDPOINT, ACTIVE_MQ_QUEUE_NAME)).thenReturn(activeMqProducer);

        task.start(createConfig());
        task.put(sinkRecords);

        verify(activeMqProducer).write(VALUE);
    }

    @Test
    void startTheProducer() {
        when(task.createProducer(ACTIVE_MQ_ENDPOINT, ACTIVE_MQ_QUEUE_NAME)).thenReturn(activeMqProducer);

        task.start(createConfig());

        verify(activeMqProducer).start();
    }

    @Test
    void stopTheProducer() {
        when(task.createProducer(ACTIVE_MQ_ENDPOINT, ACTIVE_MQ_QUEUE_NAME)).thenReturn(activeMqProducer);
        task.start(createConfig());

        task.stop();

        verify(activeMqProducer).stop();
    }

    private Map<String, String> createConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(KEY_ACTIVE_MQ_JMX_ENDPOINT, ACTIVE_MQ_ENDPOINT);
        config.put(KEY_ACTIVE_MQ_QUEUE_NAME, ACTIVE_MQ_QUEUE_NAME);
        return config;
    }

    @NotNull
    private SinkRecord createSinkRecord() {
        return new SinkRecord(KAFKA_TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, OFFSET);
    }
}
