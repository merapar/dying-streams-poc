package com.example.scheduler;

import com.example.kafka.KafkaSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

import static com.example.kafka.KafkaSettings.KAFKA_CLIENT_ID;
import static com.example.kafka.KafkaSettings.SCHEDULE_TOPIC;

public class SendMessageJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendMessageJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        LOG.info("Sending message to schedule topic");
        var messageProducer = createMessageProducer();
        messageProducer.send(new ProducerRecord<>(SCHEDULE_TOPIC, "key", System.currentTimeMillis()));
    }

    private KafkaProducer<String, Long> createMessageProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.getKafkaServer());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG + UUID.randomUUID().toString(), KAFKA_CLIENT_ID);
        return new KafkaProducer<>(properties, new StringSerializer(), new LongSerializer());
    }
}
