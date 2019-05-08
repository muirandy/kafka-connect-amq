package com.aimyourtechnology.kafka.connect.activemq.connector;

interface JmsProducer {
    void write(String value);

    void start();

    void stop();
}
