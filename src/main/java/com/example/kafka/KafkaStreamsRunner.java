package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;

import static com.example.kafka.KafkaSettings.*;

public class KafkaStreamsRunner {

    public void run() {
        createTopics();
        createStreams().start();
    }

    private void createTopics() {
        try (var adminClient = AdminClient.create(loadProperrties())) {
            adminClient.createTopics(prepareNewTopics());
        }
    }

    private List<NewTopic> prepareNewTopics() {
        return List.of(new NewTopic(SCHEDULE_TOPIC, 1, (short)1), new NewTopic(TRANSFORMING_TOPIC, 1, (short)1));
    }

    private KafkaStreams createStreams() {
        var builder = new StreamsBuilder();
        builder.stream(SCHEDULE_TOPIC, Consumed.with(new Serdes.StringSerde(), new Serdes.LongSerde())).transform(SleepingTransformer::new).to(TRANSFORMING_TOPIC, Produced.with(new Serdes.StringSerde(), new Serdes.LongSerde()));
        return new KafkaStreams(builder.build(),loadProperrties());
    }

    private static Properties loadProperrties() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.getKafkaServer());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_CLIENT_ID);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 20);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return properties;
    }
}
