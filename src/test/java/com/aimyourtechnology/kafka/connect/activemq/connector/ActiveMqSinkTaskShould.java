package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ActiveMqSinkTaskShould {
    @Test
    void haveVersion() {
        SinkTask task = new ActiveMqSinkTask();

        assertThat(task.version()).isNotEmpty();
    }
}
