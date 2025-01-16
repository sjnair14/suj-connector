FROM wurstmeister/kafka:2.12-2.3.0

COPY connector.properties /opt/kafka/config/

COPY source-connector.properties /opt/kafka/config/

COPY start-kafka.sh /usr/bin/

RUN chmod a+x /usr/bin/start-kafka.sh
