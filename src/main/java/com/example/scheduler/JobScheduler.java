package com.example.scheduler;

import org.apache.commons.lang3.tuple.Pair;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class JobScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

    public static JobScheduler instance = new JobScheduler();

    private Scheduler scheduler;

    private JobScheduler() {
        var schedulerFactory = new StdSchedulerFactory();
        try {
            LOG.info("Creating scheduler");
            this.scheduler = schedulerFactory.getScheduler();
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to create scheduler due to: %s", e.getMessage()), e);
        }
    }

    public void run() {
        try {
            LOG.info("Creating scheduler");
            this.scheduler.start();
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to start scheduler due to: %s", e.getMessage()), e);
        }
    }

    public JobScheduler scheduleJob(Class<? extends Job> job, String cronExpression, Pair<String, Object> ... objects) {
        try {
            LOG.info(String.format("Scheduling job for class %s and expression %s", job.getSimpleName(), cronExpression));
            this.scheduler.scheduleJob(buildJobDetail(job, objects), createTrigger(cronExpression));
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to schedule job due to: %s", e.getMessage()), e);
        }
        return this;
    }

    private JobDetail buildJobDetail(Class<? extends Job> job, Pair<String, Object>... objects) {
        var jobDetail = JobBuilder.newJob(job)
                .withIdentity(job.getSimpleName() + UUID.randomUUID())
            .build();

        for(var object : objects) {
            jobDetail.getJobDataMap()
                     .put(object.getKey(), object.getValue());
        }

        return jobDetail;
    }

    private CronTrigger createTrigger(String cronExpression) {
        return TriggerBuilder.newTrigger()
                .withIdentity("trigger" + UUID.randomUUID(), "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
    }

}
