package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ActiveMqSinkConnectorShould {
    @Test
    void useTheActiveMqSinkTask() {
        SinkConnector sinkConnector = new ActiveMqSinkConnector();

        Class<? extends Task> taskClass = sinkConnector.taskClass();

        assertEquals(ActiveMqSinkTask.class, taskClass);
    }

    @Test
    void t() {
        SinkConnector sinkConnector = new ActiveMqSinkConnector();
        Map<String, String> properties = new HashMap<>();

//        sinkConnector.start(properties);

        List<Map<String, String>> configs = sinkConnector.taskConfigs(1);

        Map<String, String> expectedConfig = new HashMap<>();
//        expectedConfig.put(TOPIC_CONFIG)

        assertThat(configs).containsOnly(expectedConfig);

    }
}
