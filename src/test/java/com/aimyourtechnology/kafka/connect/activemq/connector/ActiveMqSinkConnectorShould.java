package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActiveMqSinkConnectorShould {
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq_queue";
    private static final String KEY_KAFKA_TOPIC_NAME = "kafka_topic";
    private static final String ACTIVE_MQ_QUEUE_NAME = "anyOldQueue";
    private static final String KAFKA_TOPIC_NAME = "any-kafka-topic";
    private SinkConnector sinkConnector;

    @BeforeEach
    void setUp() {
        sinkConnector = new ActiveMqSinkConnector();
    }

    @Test
    void useTheActiveMqSinkTask() {
        Class<? extends Task> taskClass = sinkConnector.taskClass();

        assertEquals(ActiveMqSinkTask.class, taskClass);
    }

    @Test
    void havePropertiesSetForEachTask() {
        sinkConnector.start(buildConfiguration());

        List<Map<String, String>> configs = sinkConnector.taskConfigs(2);

        assertThat(configs).containsExactly(buildConfiguration(), buildConfiguration());
    }

    @Test
    void haveActiveMqQueueConfig() {
        ConfigDef configDef = sinkConnector.config();

        ConfigDef.ConfigKey config = configDef.configKeys().get(KEY_ACTIVE_MQ_QUEUE_NAME);
        assertThat(config.importance).isEqualTo(ConfigDef.Importance.HIGH);
        assertThat(config.type).isEqualTo(ConfigDef.Type.STRING);
        assertThat(config.documentation).isEqualTo("ActiveMQ destination Queue");
    }

    private Map<String, String> buildConfiguration() {
        HashMap<String, String> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put(KEY_ACTIVE_MQ_QUEUE_NAME, ACTIVE_MQ_QUEUE_NAME);
        expectedConfiguration.put(KEY_KAFKA_TOPIC_NAME, KAFKA_TOPIC_NAME);
        return expectedConfiguration;
    }
}
