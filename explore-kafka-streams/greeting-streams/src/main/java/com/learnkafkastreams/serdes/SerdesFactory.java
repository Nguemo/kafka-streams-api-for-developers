package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Greeting> greetingSerde() {
        return new GreetingSerde();
    }

    static public Serde<Greeting> greetingSerdeGeneric() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeSerializer<Greeting> jsonDeSerializer = new JsonDeSerializer(Greeting.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeSerializer);
    }
}
