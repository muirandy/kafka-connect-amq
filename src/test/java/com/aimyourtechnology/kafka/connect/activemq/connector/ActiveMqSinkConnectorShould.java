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
    private static final String KEY_ACTIVE_MQ_JMX_ENDPOINT = "activemq.endpoint";
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq.queue";
    private static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String STANDARD_KAFKA_CONNECT_TOPICS_KEY = "topics";

    private static final String ACTIVE_MQ_JMX_ENDPOINT = "JmxEndpoint";
    private static final String ACTIVE_MQ_QUEUE_NAME = "anyOldQueue";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    private static final String KAFKA_TOPIC_NAME = "any-kafka-topic";

    private SinkConnector sinkConnector;
    private AppVersion version;

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

    private Map<String, String> buildConfiguration() {
        HashMap<String, String> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put(KEY_ACTIVE_MQ_JMX_ENDPOINT, ACTIVE_MQ_JMX_ENDPOINT);
        expectedConfiguration.put(KEY_ACTIVE_MQ_QUEUE_NAME, ACTIVE_MQ_QUEUE_NAME);
        expectedConfiguration.put(KEY_KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS);
        expectedConfiguration.put(STANDARD_KAFKA_CONNECT_TOPICS_KEY, KAFKA_TOPIC_NAME);
        return expectedConfiguration;
    }

    @Test
    void haveActiveMqJmxEndpointConfig() {
        assertHighImportanceStringConfig(KEY_ACTIVE_MQ_JMX_ENDPOINT, "ActiveMQ JMX Endpoint");
    }

    private void assertHighImportanceStringConfig(String configKey, String documentation) {
        ConfigDef configDef = sinkConnector.config();
        ConfigDef.ConfigKey config = configDef.configKeys().get(configKey);
        assertThat(config.importance).isEqualTo(ConfigDef.Importance.HIGH);
        assertThat(config.type).isEqualTo(ConfigDef.Type.STRING);
        assertThat(config.documentation).isEqualTo(documentation);
    }

    @Test
    void haveActiveMqQueueConfig() {
        assertHighImportanceStringConfig(KEY_ACTIVE_MQ_QUEUE_NAME, "ActiveMQ destination Queue");
    }

    @Test
    void haveKafkaBootstrapServersConfig() {
        assertHighImportanceListConfig(KEY_KAFKA_BOOTSTRAP_SERVERS, "Kafka Broker(s) (eg localhost:9092)");
    }

    private void assertHighImportanceListConfig(String configKey, String documentation) {
        ConfigDef configDef = sinkConnector.config();
        ConfigDef.ConfigKey config = configDef.configKeys().get(configKey);
        assertThat(config.importance).isEqualTo(ConfigDef.Importance.HIGH);
        assertThat(config.type).isEqualTo(ConfigDef.Type.LIST);
        assertThat(config.documentation).isEqualTo(documentation);
    }

    @Test
    void haveKafkaSourceTopicConfig() {
        assertHighImportanceStringConfig(STANDARD_KAFKA_CONNECT_TOPICS_KEY, "Kafka Source Topic(s)");
    }

    @Test
    void haveVersion() {
        String version = sinkConnector.version();
        assertThat(version).isNotEmpty();
    }
}
