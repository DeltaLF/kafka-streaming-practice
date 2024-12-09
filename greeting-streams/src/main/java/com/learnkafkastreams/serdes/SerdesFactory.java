package com.learnkafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;

import com.learnkafkastreams.domain.Greeting;

public class SerdesFactory {

    static public Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }

}
