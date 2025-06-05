package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsTopology {
    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH= "greetings_spanish";

    public static Topology buildTopology(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        var greetingsSpanish= streamsBuilder
                .stream(GREETINGS_SPANISH,
                        Consumed.with(Serdes.String(),Serdes.String()));
        var mergeStream = greetingsStream.merge(greetingsSpanish);
        mergeStream
                .print(Printed.<String,String>toSysOut().withLabel("mergeStream"));



        var modifStream = greetingsStream
                .filter((key, value) -> value.length()>5)
                .peek((key, value) -> {
                    log.info("After filter : key :{}, value : {}",key,value);
                })
                .mapValues(((readOnlyKey, value) -> value.toUpperCase()))
                        .peek((key, value) -> {
                            log.info("After mapValues : key :{}, value : {}",key,value);
                        });

               // .filter((key, value) -> value.length()>5);
        greetingsStream.flatMap((key, value) -> {
                           var newVal = Arrays.asList(value.split(""));
                          return newVal.stream()
                                   .map(val-> KeyValue.pair(key,value.toUpperCase()))
                                   .collect(Collectors.toList());
                       });

        modifStream
                .print(Printed.<String,String>toSysOut().withLabel("modifStream"));

        modifStream
                .to(GREETINGS_UPPERCASE,
                        Produced.with(Serdes.String(),Serdes.String()));

       return streamsBuilder.build();
    }
}
