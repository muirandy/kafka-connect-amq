package com.aimyourtechnology.kafka.connect.activemq.connector;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.jms.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ActiveMqProducerShould {

    private static final String MESSAGE = "Hello Active MQ";
    private static final String QUEUE_NAME = "ActiveMqQueueName";
    private static final String ACTIVE_MQ_CONNECTION_STRING = "vm://localhost";

    @Spy
    private ActiveMqProducer activeMqProducer = new ActiveMqProducer(ACTIVE_MQ_CONNECTION_STRING, QUEUE_NAME);

    @Mock
    private ActiveMQConnectionFactory activeMQConnectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Session session;

    @Mock
    private MessageProducer producer;

    @Mock
    private TextMessage textMessage;

    @Mock
    private Queue queue;

    @Captor
    private ArgumentCaptor<TextMessage> textMessageCaptor;

    @BeforeEach
    void setUp() throws JMSException {
        when(activeMQConnectionFactory.createConnection()).thenReturn(connection);

        when(activeMqProducer.createConnectionFactory(ACTIVE_MQ_CONNECTION_STRING)).thenReturn(activeMQConnectionFactory);
    }

    @Test
    void writeToActiveMq() throws JMSException {
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createQueue(QUEUE_NAME)).thenReturn(queue);
        when(session.createProducer(queue)).thenReturn(producer);
        when(session.createTextMessage(MESSAGE)).thenReturn(textMessage);
        when(textMessage.getText()).thenReturn(MESSAGE);

        activeMqProducer.write(MESSAGE);

        verify(producer).send(textMessageCaptor.capture());
        assertThat(textMessageCaptor.getValue().getText()).isEqualTo(MESSAGE);
    }

    @Test
    void startTheActiveMqConnection() throws JMSException {
        activeMqProducer.start();

        verify(connection).start();
    }
}