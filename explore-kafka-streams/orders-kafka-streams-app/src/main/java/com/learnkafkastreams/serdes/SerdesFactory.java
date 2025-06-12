package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Order> orderSerde() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeSerializer<Order> jsonDeSerializer = new JsonDeSerializer(Order.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeSerializer);
    }
}
