package com.aimyourtechnology.kafka.connect.activemq.sink;

import com.eclipsesource.json.JsonObject;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ActiveMqSinkServiceTest {
    private static final String ACTIVEMQ_IMAGE = "rmohr/activemq:latest";

    private static final String INPUT_TOPIC = "modify.op.msgs";
    private static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    private static final int ACTIVE_MQ_JMS_PORT = 61616;
    private static final String ACTIVE_MQ_QUEUE_NAME = "TEST.FOO";

    private static final String MESSAGE_CONTENT = "A message";
    private static final String STANDARD_KAFKA_CONNECT_TOPICS_KEY = "topics";
    private static final String CONNECTOR_NAME ="banana";
    private static final String CONNECTOR_CLASS =
            "com.aimyourtechnology.kafka.connect.activemq.connector.ActiveMqSinkConnector";

    private static final String KEY_ACTIVE_MQ_JMX_ENDPOINT = "activemq.endpoint";
    private static final String KEY_ACTIVE_MQ_QUEUE_NAME = "activemq.queue";
    private static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String KEY_CONNECTOR_CLASS = "connector.class";
    private static final String KEY_CONNECTOR_NAME = "name";
    private static final String KEY_CONFIG = "config";

    @Container
    protected static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("5.2.1").withEmbeddedZookeeper()
            .waitingFor(Wait.forLogMessage(".*started.*\\n", 1));

    @Container
    protected static final GenericContainer ACTIVE_MQ_CONTAINER = new GenericContainer(ACTIVEMQ_IMAGE)
            .withNetwork(KAFKA_CONTAINER.getNetwork())
            .withExposedPorts(ACTIVE_MQ_JMS_PORT);

    @Container
    protected GenericContainer kafkaConnectContainer = new GenericContainer(
            new ImageFromDockerfile()
                    .withFileFromFile("Dockerfile", getDockerfileFile())
                    .withFileFromFile("kafka-connect-amq-1.0-SNAPSHOT-jar-with-dependencies.jar",
                            new File("target/kafka-connect-amq-1.0-SNAPSHOT-jar-with-dependencies.jar")))
            .withEnv(calculateConnectEnvProperties())
            .withNetwork(KAFKA_CONTAINER.getNetwork())
            .waitingFor(Wait.forLogMessage(".*Finished starting connectors and tasks.*\\n", 1));


    private File getDockerfileFile() {
        return new File("./Dockerfile");
    }

    private Map<String, String> calculateConnectEnvProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECT_BOOTSTRAP_SERVERS", getKafkaBootstrapServers());
        properties.put("CONNECT_GROUP_ID", "service-test-connect-group");
        properties.put("CONNECT_REST_PORT", "8083");
        properties.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        properties.put("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        properties.put("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        properties.put("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter");
        properties.put("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter");
        properties.put("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        properties.put("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        properties.put("CONNECT_CONFIG_STORAGE_TOPIC", "docker-connect-configs");
        properties.put("CONNECT_OFFSET_STORAGE_TOPIC", "docker-connect-offsets");
        properties.put("CONNECT_STATUS_STORAGE_TOPIC", "docker-connect-status");
        properties.put("CONNECT_REST_ADVERTISED_HOST_NAME", "connect");
        properties.put("CONNECT_PLUGIN_PATH", "/usr/share/java/kafka-amq2");

        createKafkaTopics();

        return properties;
    }

    private void createKafkaTopics() {
        AdminClient adminClient = AdminClient.create(getKafkaProperties());

        CreateTopicsResult createTopicsResult = adminClient.createTopics(getKafkaTopicsToCreate(),
                new CreateTopicsOptions().timeoutMs(5000));
        Map<String, KafkaFuture<Void>> futureResults = createTopicsResult.values();
        futureResults.values().forEach(f -> {
            try {
                f.get(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        adminClient.close();
    }

    protected static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ActiveMqSinkServiceTest.class.getName());
        return props;
    }

    private Collection<NewTopic> getKafkaTopicsToCreate() {
        return getKafkaTopicNames().stream()
                .map(n -> new NewTopic(n, 1, (short) 1))
                .collect(Collectors.toList());
    }

    protected List<String> getKafkaTopicNames() {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(INPUT_TOPIC);
        topicNames.add("docker-connect-configs");
        topicNames.add("docker-connect-offsets");
        topicNames.add("docker-connect-status");
        return topicNames;
    }

    @BeforeEach
    public void assertContainersAreRunning() {
        assertTrue(KAFKA_CONTAINER.isRunning());
        assertTrue(ACTIVE_MQ_CONTAINER.isRunning());
        assertTrue(kafkaConnectContainer.isRunning());
    }

    @Test
    public void transfersMessageOntoActiveMq() throws ExecutionException, InterruptedException {
        configurePlugin();
        writeStringToKafkaInputTopic();
        waitForConnectToDoItsMagic();
        assertJmsMessageArrivedOnOutputMqQueue();
    }

    private void configurePlugin() {
        HttpPost httpPost = new HttpPost(getUriForConnectEndpoint());
        HttpEntity httpEntity = new StringEntity(getPayload(), APPLICATION_JSON);

        httpPost.setEntity(httpEntity);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            httpClient.execute(httpPost).close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getPayload() {
        JsonObject config = new JsonObject()
                .add(KEY_CONNECTOR_CLASS, CONNECTOR_CLASS)
                .add(KEY_ACTIVE_MQ_JMX_ENDPOINT, getActiveMqJmxEndpoint())
                .add(KEY_ACTIVE_MQ_QUEUE_NAME, ACTIVE_MQ_QUEUE_NAME)
                .add(STANDARD_KAFKA_CONNECT_TOPICS_KEY, INPUT_TOPIC)
                .add(KEY_KAFKA_BOOTSTRAP_SERVERS, getKafkaBootstrapServers());
        return new JsonObject()
                .add(KEY_CONNECTOR_NAME, CONNECTOR_NAME)
                .add(KEY_CONFIG, config)
                .toString();
    }

    private String getKafkaBootstrapServers() {
        return KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092";
    }

    private String getActiveMqJmxEndpoint() {
        return "tcp://" + ACTIVE_MQ_CONTAINER.getNetworkAliases().get(0) + ":61616";
    }

    private String getUriForConnectEndpoint() {
        String port = findExposedPortForInternalPort(kafkaConnectContainer, 8083);
        return "http://localhost:" + port + "/connectors";
    }

    protected void writeStringToKafkaInputTopic() throws InterruptedException, ExecutionException {
        new KafkaProducer<String, String>(getKafkaProperties()).send(createProducerRecord()).get();
    }

    protected ProducerRecord createProducerRecord() {
        return new ProducerRecord(INPUT_TOPIC, createKeyForInputMessage(), createInputMessage());
    }

    private String createKeyForInputMessage() {
        return "key";
    }

    private String createInputMessage() {
        return MESSAGE_CONTENT;
    }

    private void waitForConnectToDoItsMagic() {
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void assertJmsMessageArrivedOnOutputMqQueue() {
        ActiveMqConsumer consumer = new ActiveMqConsumer(readActiveMqPort());
        String messageFromActiveMqQueue = consumer.run();
        assertEquals(MESSAGE_CONTENT, messageFromActiveMqQueue);
    }

    private String readActiveMqPort() {
        return findExposedPortForInternalPort(ACTIVE_MQ_CONTAINER, ACTIVE_MQ_JMS_PORT);
    }

    private String findExposedPortForInternalPort(GenericContainer activeMqContainer, int internalPort) {
        Map<ExposedPort, Ports.Binding[]> bindings = getActiveMqBindings(activeMqContainer);
        ExposedPort port = bindings.keySet().stream().filter(k -> internalPort == k.getPort())
                .findFirst().get();

        Ports.Binding[] exposedBinding = bindings.get(port);
        Ports.Binding binding = exposedBinding[0];
        return binding.getHostPortSpec();
    }

    private Map<ExposedPort, Ports.Binding[]> getActiveMqBindings(GenericContainer activeMqContainer) {
        return activeMqContainer.getContainerInfo().getNetworkSettings().getPorts().getBindings();
    }

    @AfterEach
    public void tearDown() {
        writeContainerLogsToStdOut();
    }

    private void writeContainerLogsToStdOut() {
        System.out.println("Kafka Logs = " + KAFKA_CONTAINER.getLogs());
        System.out.println("Active MQ Logs = " + ACTIVE_MQ_CONTAINER.getLogs());
        System.out.println("Kafka Connect Logs = " + kafkaConnectContainer.getLogs());
    }
}
