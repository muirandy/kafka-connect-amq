package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ActiveMqSinkConnectorShould {
    @Test
    void useTheActiveMqSinkTask() {
        SinkConnector sinkConnector = new ActiveMqSinkConnector();

        Class<? extends Task> taskClass = sinkConnector.taskClass();

        assertEquals(ActiveMqSinkTask.class, taskClass);
    }


}
