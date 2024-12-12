package com.learnkafkastreams.exceptionhandler;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("### StreamsProcessorCustomErrorHandler exception is:{}",
                exception.getMessage(), exception);
        if (exception instanceof StreamsException) {
            var cause = exception.getCause();
            if (cause.getMessage().equals("Transient Error")) { // a custom transient error
                // retry only when it's a transient error
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT; // shutdown current thread instead of the whole app
    }

}
