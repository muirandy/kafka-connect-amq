# Kafka Connect Active MQ Sink
This is a Kafka Connector to produce messages onto Active MQ from a Kafka Topic.

## Limitations
* Only writes to a single target Queue (Topics not supported)
* No Kafka Connect SMTs available

## How to run the Service Tests
The Service test will build and run the connector against Kafka and ActiveMQ.
You do not need to install ActiveMQ (or even Kafka), but you will need Docker.

1) Build the "fat" JAR for the MQ Connector:
mvn package

2) Run ActiveMqSinkServiceTest 