package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Order> orderSerde() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeSerializer<Order> jsonDeSerializer = new JsonDeSerializer(Order.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeSerializer);
    }


    static public Serde<Revenue> revenueSerdeSerde() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeSerializer<Revenue> jsonDeSerializer = new JsonDeSerializer(Revenue.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeSerializer);
    }
}
