package com.example.scheduler;

import com.example.kafka.MonitoredKafkaStreams;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsWatchdogJob implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsWatchdogJob.class);
    public static final String STREAMS_VARIABLE = "streams";


    public KafkaStreamsWatchdogJob(){
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        MonitoredKafkaStreams monitoredKafkaStreams = getStreams(jobExecutionContext);

        var healthyStreams = monitoredKafkaStreams.getHealth();

        float healthyPercentage = (healthyStreams.getLeft() * 100.0f) / healthyStreams.getRight();

        LOG.info("{} of the {} stream threads ({}%) are healthy", healthyStreams.getLeft(), healthyStreams.getRight(), healthyPercentage);
        int requiredHealthPercentage = 50;
        if (healthyPercentage < requiredHealthPercentage){
            LOG.error("{}% percentage healthy stream threads is less than the required {} percentage. Stopping application", healthyPercentage, requiredHealthPercentage);
            System.exit(0);
        }
    }

    private MonitoredKafkaStreams getStreams(JobExecutionContext jobExecutionContext) {
        return (MonitoredKafkaStreams) jobExecutionContext.getJobDetail().getJobDataMap().get(STREAMS_VARIABLE);
    }
}
