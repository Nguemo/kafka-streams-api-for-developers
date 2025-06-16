package com.learnkafkastreams.exceptionHandeler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;
/**
 * This class is added in kafka configuration class
 */
@Slf4j
public class StreamSerializationExceptionHandler implements ProductionExceptionHandler {
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {

        log.error("Serialization Exception is :{} , and the kafka record is : {}  ",exception.getMessage(),record,exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
