package com.learnkafkastreams.exceptionHandeler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
/**
 * This class is used like
 LogAndContinueExceptionHandler.class //If we have an error the server will continue and not stop
 *
 */
import java.util.Map;
@Slf4j
public class StreamExceptionHandeler implements DeserializationExceptionHandler {

    int errorCounter = 0 ;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception is :{} , and the kafka record is : {}  ",exception.getMessage(),record,exception);
       log.error("errorCounter : {} ",errorCounter);
        if(errorCounter < 2){
            errorCounter ++ ;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
