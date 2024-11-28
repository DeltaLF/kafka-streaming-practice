package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGSII = "greetings2"; // for merging

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {

        // from greetings topic to greetings_uppercase topic

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), Serdes.String()));
        var greetingsStreamII = streamsBuilder
                .stream(GREETINGSII,
                        Consumed.with(Serdes.String(), Serdes.String()));

        var mergedStream = greetingsStream.merge(greetingsStreamII);

        mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

        var modifiedStream = mergedStream
                .filter((key, value) -> value.length() > 5)
                .peek((key, value) -> {
                    log.info("# peek stream key:{}, value:{}", key, value);
                })
                .mapValues((readOnlyKey, value) -> value.toUpperCase());
        // .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
        // .flatMap((key, value) -> {
        // var splitValue = Arrays.asList(value.split(""));
        // return splitValue
        // .stream()
        // .map(val -> KeyValue.pair(key, val.toUpperCase()))
        // .collect(Collectors.toList());
        // });

        mergedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
