package com.example;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

    private Scheduler scheduler;

    public JobScheduler() {
        var schedulerFactory = new StdSchedulerFactory();
        try {
            LOG.info("Starting scheduler");
            this.scheduler = schedulerFactory.getScheduler();
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to start scheduler due to: %s", e.getMessage()), e);
        }
    }

    public JobScheduler scheduleJob(Class<? extends Job> job, String cronExpression) {
        try {
            LOG.info(String.format("Scheduling job for class %s and expression %s", job.getSimpleName(), cronExpression));
            this.scheduler.scheduleJob(buildJobDetail(job), createTrigger(cronExpression));
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to schedule job due to: %s", e.getMessage()), e);
        }
        return this;
    }

    public void start() {
        try {
            this.scheduler.start();
        } catch (SchedulerException e) {
            LOG.error(String.format("Failed to start scheduler due to: %s", e.getMessage()), e); }
    }

    private JobDetail buildJobDetail(Class<? extends Job> job) {
        return JobBuilder.newJob(job)
                .withIdentity(job.getSimpleName())
                .build();
    }

    private CronTrigger createTrigger(String cronExpression) {
        return TriggerBuilder.newTrigger()
                .withIdentity("trigger", "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
    }

}
