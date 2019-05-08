package com.aimyourtechnology.kafka.connect.activemq.connector;

public interface JmsProducer {
    void write(String value);
}
