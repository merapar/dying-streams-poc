package com.example;

import com.example.kafka.EmbeddedKafkaRunner;
import com.example.kafka.KafkaStreamsRunner;
import com.example.scheduler.JobScheduler;
import com.example.scheduler.KafkaStreamsWatchdogJob;
import com.example.scheduler.SendMessageJob;
import org.apache.commons.lang3.tuple.Pair;

public class DyingStreamsPocApplication {

    public static void main(String[] args) {
        EmbeddedKafkaRunner.runIfNeeded();
        var kafkaStreamsRunner = new KafkaStreamsRunner();
        kafkaStreamsRunner.run();

        JobScheduler.instance.scheduleJob(KafkaStreamsWatchdogJob.class, "*/10 * * ? * *", Pair.of(KafkaStreamsWatchdogJob.STREAMS_VARIABLE, kafkaStreamsRunner.getMonitoredKafkaStreams()));
        JobScheduler.instance.scheduleJob(SendMessageJob.class, "*/11 * * ? * *");
        JobScheduler.instance.scheduleJob(SendMessageJob.class, "*/5 * * ? * *");
        JobScheduler.instance.run();

    }

}
