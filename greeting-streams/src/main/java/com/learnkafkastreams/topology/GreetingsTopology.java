package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGSII = "greetings2"; // for merging

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {

        // from greetings topic to greetings_uppercase topic

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // KStream<String, String> mergedStream =
        // getStringGreetingKStream(streamsBuilder);
        KStream<String, Greeting> mergedStream = getCustomGreetingKStream(streamsBuilder);

        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("mergedStream"));

        // var modifiedStream = streamOperator(mergedStream);

        var modifiedStream = streamOperatorError(mergedStream);

        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE,
                Produced.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGenerics()));

        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> streamOperator(KStream<String, Greeting> mergedStream) {
        var modifiedStream = mergedStream
                .peek((key, value) -> {
                    log.info("# peek stream key:{}, value:{}", key, value);
                })
                // .filter((key, value) -> value.getMessage().length() > 5)

                .mapValues(
                        (readOnlyKey, value) -> new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp()));
        // .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
        // .flatMap((key, value) -> {
        // var splitValue = Arrays.asList(value.split(""));
        // return splitValue
        // .stream()
        // .map(val -> KeyValue.pair(key, val.toUpperCase()))
        // .collect(Collectors.toList());
        // });
        return modifiedStream;
    }

    private static KStream<String, Greeting> streamOperatorError(KStream<String, Greeting> mergedStream) {

        return mergedStream
                .peek((key, value) -> {
                    log.info("# peek stream key:{}, value:{}", key, value);
                })
                .mapValues(
                        (readOnlyKey, value) -> {
                            if (value.getMessage().equals("Transient Error")) {
                                try {
                                    throw new IllegalStateException(value.getMessage());
                                } catch (Exception e) {
                                    log.error("Exception in exploreErrors : {}", e.getMessage(), e);
                                    return null;
                                }
                            }
                            return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
                        })
                .filter((key, value) -> key != null && value != null);

    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS
                // Consumed.with(Serdes.String(), Serdes.String())
                );

        greetingsStream.peek((key, value) -> {
            log.info("#####~~ peek stream key:{}, value:{}", key, value);
        });
        KStream<String, String> greetingsStreamII = streamsBuilder
                .stream(GREETINGSII
                // Consumed.with(Serdes.String(), Serdes.String())
                );
        greetingsStreamII.peek((key, value) -> {
            log.info("###### peek stream key:{}, value:{}", key, value);
        });

        return greetingsStream.merge(greetingsStreamII);
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGenerics()));
        var greetingsStreamII = streamsBuilder
                .stream(GREETINGSII,
                        Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGenerics()));

        return greetingsStream.merge(greetingsStreamII);
    }
}
