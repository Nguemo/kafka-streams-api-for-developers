package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;


@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_stores";
    public static final String GENERAL_ORDERS_PRICE = "general_orders_price";
    public static final String RESTAURANT_ORDERS_PRICE = "restaurant_stores_price";


    public static Topology builTopology(){
        Predicate<String,Order> generalPredicate = ((key, order) -> order.orderType().equals(OrderType.GENERAL));
        Predicate<String,Order> restaurantPredicate = ((key, order) -> order.orderType().equals(OrderType.RESTAURANT));

        ValueMapper<Order, Revenue> revenuMapper = order ->new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var orderStream =  streamsBuilder.stream(ORDERS, /// consume the stream from ORDERS TOPIC
                Consumed.with(Serdes.String(), SerdesFactory.orderSerde())
                );
        orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders")); //Affichage console

        orderStream.split(Named.as("General-restaurant-stream")) /// diriger le stream avec le nom General-restorant-stream
                .branch(generalPredicate, /// Trier selon generalPredicate
                        Branched.withConsumer(generalPredicateStream ->{ //Consommer le stream apres triage
                            generalPredicateStream.print(Printed.<String,Order>toSysOut().withLabel("General Stream"));
                            generalPredicateStream
                                    .to(GENERAL_ORDERS,
                                    Produced.with(Serdes.String(),SerdesFactory.orderSerde())); //Publication du resultat de traitement sur la Toipic GENERAL_ORDERS_PRICE

                        }))
                .branch(restaurantPredicate, /// Trier selon restaurant Predicate
                        Branched.withConsumer(restaurantPredicateStream ->{
                            restaurantPredicateStream.print(Printed.<String,Order>toSysOut().withLabel("Restaurant Stream"));
                            restaurantPredicateStream.to(RESTAURANT_ORDERS,
                                    Produced.with(Serdes.String(),SerdesFactory.orderSerde())); //Publication du resultat de traitement sur la Toipic RESTAURANT_ORDERS_PRICE
                        }));


        orderStream.split(Named.as("General-restaurant-stream-price")) /// diriger le stream avec le nom General-restorant-stream
                .branch(generalPredicate, /// Trier selon generalPredicate
                        Branched.withConsumer(generalPredicateStream ->{ //Consommer le stream apres triage
                            generalPredicateStream.print(Printed.<String,Order>toSysOut().withLabel("General Stream price"));
                            generalPredicateStream
                                    .mapValues((readOnlyKey, value) -> revenuMapper.apply(value)) //recuperation des objets revenus seulement
                                    .to(GENERAL_ORDERS_PRICE,
                                            Produced.with(Serdes.String(),SerdesFactory.revenueSerdeSerde())); //Publication du resultat de traitement sur la Toipic GENERAL_ORDERS

                        }))
                .branch(restaurantPredicate, /// Trier selon restaurant Predicate
                        Branched.withConsumer(restaurantPredicateStream ->{
                            restaurantPredicateStream.print(Printed.<String,Order>toSysOut().withLabel("Restaurant Stream price"));
                            restaurantPredicateStream
                                    .mapValues((readOnlyKey, value) -> revenuMapper.apply(value))  //recuperation des objets revenus seulement
                                    .to(RESTAURANT_ORDERS_PRICE,
                                    Produced.with(Serdes.String(),SerdesFactory.revenueSerdeSerde())); //Publication du resultat de traitement sur la Toipic RESTAURANT_ORDERS
                        }));


        return streamsBuilder.build();
    }
}
