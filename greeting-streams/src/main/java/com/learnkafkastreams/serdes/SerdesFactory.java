package com.learnkafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.learnkafkastreams.domain.Greeting;

public class SerdesFactory {

    static public Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }

    static public Serde<Greeting> greetingSerdesUsingGenerics() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
