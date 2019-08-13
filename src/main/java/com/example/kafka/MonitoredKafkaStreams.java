package com.example.kafka;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Arrays;
import java.util.Properties;

public class MonitoredKafkaStreams extends KafkaStreams {

    public MonitoredKafkaStreams(Topology topology, Properties props) {
        super(topology, props);
    }

    public Pair<Integer, Integer> getHealth() {
        var streamThreads = Arrays.asList(threads);
        var healthyThreadCount = (int)streamThreads.stream().filter(x -> x.state() != StreamThread.State.DEAD).count();
        return Pair.of(healthyThreadCount, streamThreads.size());
    }
}
