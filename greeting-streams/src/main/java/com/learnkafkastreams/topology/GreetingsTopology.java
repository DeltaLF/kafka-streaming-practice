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

        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.greetingSerdes()));

        return streamsBuilder.build();
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
                        Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
        var greetingsStreamII = streamsBuilder
                .stream(GREETINGSII,
                        Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));

        return greetingsStream.merge(greetingsStreamII);
    }
}
