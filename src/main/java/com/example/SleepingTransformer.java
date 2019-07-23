package com.example;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class SleepingTransformer implements Transformer<String, Long, KeyValue<String, Long>> {

    private static final Duration PAUSE = Duration.ofSeconds(240);

    private static final Logger LOG = LoggerFactory.getLogger(SleepingTransformer.class);

    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public KeyValue<String, Long> transform(String key, Long value) {
        long timeForWakeUp = value + PAUSE.toMillis();
        long pause = timeForWakeUp - System.currentTimeMillis();
        if(pause >  0) {
            try {
                LOG.info("Sleeping for {}s", pause / 1000);
                Thread.sleep(pause);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {

    }
}
