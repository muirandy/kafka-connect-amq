package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActiveMqSinkConnector extends SinkConnector {
    private static final String KEY_ACTIVE_MQ_JMX_ENDPOINT = "activemq.endpoint";
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq.queue";
    private static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String KEY_KAFKA_TOPIC_NAME = "kafka.topic";

    private Map<String, String> properties;
    private ConfigDef configDef;

    public ActiveMqSinkConnector() {
        configDef = buildConfigDef();
    }

    private ConfigDef buildConfigDef() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(KEY_ACTIVE_MQ_JMX_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "ActiveMQ JMX Endpoint");
        configDef.define(KEY_ACTIVE_MQ_QUEUE_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "ActiveMQ destination Queue");
        configDef.define(KEY_KAFKA_BOOTSTRAP_SERVERS, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Kafka Broker(s) (eg localhost:9092)");
        configDef.define(KEY_KAFKA_TOPIC_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka Source Topic");
        return configDef;
    }

    @Override
    public void start(Map<String, String> props) {
        properties = props;
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
        return AppVersion.getVersion();
    }
}
