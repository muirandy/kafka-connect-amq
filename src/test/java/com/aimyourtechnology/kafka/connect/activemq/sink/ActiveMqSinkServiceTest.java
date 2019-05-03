package com.aimyourtechnology.kafka.connect.activemq.sink;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ActiveMqSinkServiceTest {
    private static final String KAFKA_CONNECT_IMAGE = "sns2-system-tests_connect:latest";
    private static final String ACTIVEMQ_IMAGE = "rmohr/activemq:latest";

    private static final String INPUT_TOPIC = "modify.op.msgs";
    private static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    private static final int ACTIVE_MQ_JMS_PORT = 61616;

    private static final String MESSAGE_CONTENT = "A message";

    @Container
    protected static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("5.2.1").withEmbeddedZookeeper()
            .waitingFor(Wait.forLogMessage(".*started.*\\n", 1));

    @Container
    protected static final GenericContainer ACTIVE_MQ_CONTAINER = new GenericContainer(ACTIVEMQ_IMAGE)
            .withNetwork(KAFKA_CONTAINER.getNetwork());

    @Container
    protected GenericContainer kafkaConnectContainer = new GenericContainer(KAFKA_CONNECT_IMAGE)
            .withEnv(calculateConnectEnvProperties())
            .withNetwork(KAFKA_CONTAINER.getNetwork());

    private Map<String, String> calculateConnectEnvProperties() {
        createKafkaTopics();

        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECT_BOOTSTRAP_SERVERS", KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092");
        properties.put("CONNECT_GROUP_ID", "service-test-connect-group");
        properties.put("CONNECT_REST_PORT", "8083");
        properties.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        properties.put("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        properties.put("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        properties.put("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");

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
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
        adminClient.close();
    }

    protected static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
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
        writeStringToKafkaInputTopic();
        assertJmsMessageArrivedOnOutputMqQueue();
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
