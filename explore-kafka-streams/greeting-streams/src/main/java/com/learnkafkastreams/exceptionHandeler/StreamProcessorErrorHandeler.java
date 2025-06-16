package com.learnkafkastreams.exceptionHandeler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * This class is used in Topology class to handeler the Exception
 * For more Example see main branch
 */

@Slf4j
public class StreamProcessorErrorHandeler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Exception in the Application :{} ",exception.getMessage(),exception);
        if(exception instanceof StreamsException){
            var cause = exception.getCause();
            if (cause.getCause().equals("Transient Error")){
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
}
