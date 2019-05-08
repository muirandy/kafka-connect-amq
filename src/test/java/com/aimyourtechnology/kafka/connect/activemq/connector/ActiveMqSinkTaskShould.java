package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ActiveMqSinkTaskShould {
    private static final String KAFKA_TOPIC = "aKafkaTopic";
    private static final int PARTITION = 42;
    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final String KEY = "KafkaMessageKey";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "<some><xml><perhaps/></xml></some>";
    private static final long OFFSET = 666L;


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

        when(task.createProducer()).thenReturn(activeMqProducer);

        task.start(Collections.emptyMap());
        task.put(sinkRecords);

        verify(activeMqProducer).write(VALUE);
    }

    @NotNull
    private SinkRecord createSinkRecord() {
        return new SinkRecord(KAFKA_TOPIC, PARTITION, KEY_SCHEMA, KEY, VALUE_SCHEMA, VALUE, OFFSET);
    }
}
