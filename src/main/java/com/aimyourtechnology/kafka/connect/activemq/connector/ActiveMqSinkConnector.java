package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActiveMqSinkConnector extends SinkConnector {
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq_queue";

    private Map<String, String> properties;
    private ConfigDef configDef;

    public ActiveMqSinkConnector() {
        configDef = buildConfigDef();
    }

    private ConfigDef buildConfigDef() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(KEY_ACTIVE_MQ_QUEUE_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "ActiveMQ destination Queue");
        return configDef;
    }

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
            configs.add(createConfigurationMapForTask());
        return configs;
    }

    private Map<String, String> createConfigurationMapForTask() {
        return new HashMap<>(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return configDef;
    }

    @Override
    public String version() {
        return null;
    }
}
