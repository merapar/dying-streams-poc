package com.example.scheduler;

import com.example.kafka.KafkaSettings;
import com.google.common.base.Suppliers;
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
import java.util.function.Supplier;

import static com.example.kafka.KafkaSettings.KAFKA_CLIENT_ID;
import static com.example.kafka.KafkaSettings.SCHEDULE_TOPIC;

public class SendMessageJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendMessageJob.class);

    private static Properties properties;
    private static Supplier<KafkaProducer<String, Long>> producerSupplier = Suppliers.memoize(SendMessageJob::createMessageProducer)::get;

    static {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.getKafkaServer());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KAFKA_CLIENT_ID + UUID.randomUUID());
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        LOG.info("Sending message to schedule topic");
        producerSupplier.get().send(new ProducerRecord<>(SCHEDULE_TOPIC, UUID.randomUUID().toString(), System.currentTimeMillis()));
    }

    private static KafkaProducer<String, Long> createMessageProducer() {
        return new KafkaProducer<>(properties, new StringSerializer(), new LongSerializer());
    }
}
