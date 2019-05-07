package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class ActiveMqSinkTask extends SinkTask {
    @Override
    public String version() {
        return AppVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public void put(Collection<SinkRecord> collection) {

    }

    @Override
    public void stop() {

    }
}
