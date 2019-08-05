package com.example;

import com.example.kafka.EmbeddedKafkaRunner;
import com.example.kafka.KafkaStreamsRunner;
import com.example.scheduler.JobScheduler;

public class DyingStreamsPocApplication {

    public static void main(String[] args) {
        EmbeddedKafkaRunner.runIfNeeded();
        new KafkaStreamsRunner().run();
        new JobScheduler().run();
    }

}
