package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActiveMqSinkConnector extends SinkConnector {
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> map) {
        properties = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ActiveMqSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i=0; i<maxTasks; i++)
            configs.add(createMap());
        return configs;
    }

    private Map<String, String> createMap() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.putAll(properties);
        return configuration;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return null;
    }
}
