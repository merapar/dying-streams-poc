package com.example.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedKafkaRunner {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaRunner.class);

    private static final SharedKafkaTestResource SHARED_KAFKA_TEST_RESOURCE = new SharedKafkaTestResource().withBrokers(1);

    public static void runIfNeeded() {
        if (System.getenv("EMBEDDED_KAFKA") != null) {
            try {
                SHARED_KAFKA_TEST_RESOURCE.beforeAll(null);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> SHARED_KAFKA_TEST_RESOURCE.afterAll(null)));
                KafkaSettings.setKafkaServer(SHARED_KAFKA_TEST_RESOURCE.getKafkaConnectString());
            } catch (Exception e) {
                LOG.error(String.format("Unable to start embedded Kafka due to %s", e.getMessage()), e);
            }
        }
    }

}
