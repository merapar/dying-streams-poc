package com.example;

public class DyingStreamsPocApplication {

    public static void main(String[] args) {
        new JobScheduler().scheduleJob(SendMessageJob.class, "*/10 * * ? * *").start();
        new KafkaStreamsRunner().run();
    }







}
