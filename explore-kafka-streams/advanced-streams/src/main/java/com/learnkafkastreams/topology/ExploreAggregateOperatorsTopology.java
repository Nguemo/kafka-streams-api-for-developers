package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

       var inputStream =  streamsBuilder
                .stream(AGGREGATE,Consumed.with(Serdes.String(),Serdes.String())); ///Lecture sur la topic AGGREGATE
        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        var groupedString =  inputStream
                .groupByKey(Grouped.with(Serdes.String(),Serdes.String())); //Regroupement par clé
        exploreCount(groupedString); //Contage

        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupedString) {
       var coutByAlphabet =  groupedString
                .count(Named.as("count-per-alphabet"));
        coutByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("word-count-per-alphabet"));
    }


}
