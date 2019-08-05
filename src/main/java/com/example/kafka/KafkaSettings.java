package com.example.kafka;

public class KafkaSettings {

    public static final String SCHEDULE_TOPIC = "schedule_topic";
    private static String KAFKA_SERVER = "localhost:9092";
    public static final String KAFKA_CLIENT_ID = "client_id";
    static final String TRANSFORMING_TOPIC = "transforming_topic";

    public static String getKafkaServer() {
        return KAFKA_SERVER;
    }

    static void setKafkaServer(String kafkaServer) {
        KAFKA_SERVER = kafkaServer;
    }

}
