FROM confluentinc/cp-kafka-connect:5.3.0

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN mkdir -p /opt/kafka-connect/jars
RUN mkdir -p /usr/share/java/kafka-amq2/

WORKDIR target

ADD kafka-connect-amq-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/kafka-connect/jars/
RUN ln -s /opt/kafka-connect/jars/kafka-connect-amq-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/share/java/kafka-amq2/
