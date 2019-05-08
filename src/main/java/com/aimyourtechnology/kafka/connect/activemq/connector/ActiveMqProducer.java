package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

class ActiveMqProducer implements JmsProducer {
    private final String activeMqEndpoint;
    private final String activeMqQueueName;

    ActiveMqProducer(String activeMqEndpoint, String activeMqQueueName) {
        this.activeMqEndpoint = activeMqEndpoint;
        this.activeMqQueueName = activeMqQueueName;
    }

    @Override
    public void start() {
        ActiveMQConnectionFactory factory = createConnectionFactory(activeMqEndpoint);
        try {
            Connection connection = factory.createConnection();
            connection.start();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public void write(String message) {
        ActiveMQConnectionFactory factory = createConnectionFactory(activeMqEndpoint);
        try {
            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(activeMqQueueName);
            MessageProducer producer = session.createProducer(queue);
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    ActiveMQConnectionFactory createConnectionFactory(String activeMqConnectionString) {
        return new ActiveMQConnectionFactory(activeMqConnectionString);
    }
}
