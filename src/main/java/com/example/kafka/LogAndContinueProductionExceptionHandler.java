package com.example.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogAndContinueProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogAndContinueProductionExceptionHandler.class);


    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
        if (e instanceof UnknownProducerIdException){
            LOG.error("UnknownProducerIdException during processing record. These exceptions will not be continued", e);
            return ProductionExceptionHandlerResponse.FAIL;
        }
        LOG.error("Exception during processing record. Flow will be continued", e);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
