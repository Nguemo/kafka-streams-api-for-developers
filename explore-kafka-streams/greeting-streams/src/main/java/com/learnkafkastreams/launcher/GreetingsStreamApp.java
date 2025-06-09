package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.GreetingsTopology;
import com.learnkafkastreams.topology.ListTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.GreetingsTopology.*;

@Slf4j
public class GreetingsStreamApp {

   public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");



       Optional.of(List.of(GREETINGS, GREETINGS_UPPERCASE, GREETINGS_SPANISH).stream()
               .filter(Predicate.not(ListTopics.listerTopic()::contains))
               .toList())
                       .filter(list -> !list.isEmpty())
                               .ifPresent(missing -> createTopics(properties,missing));


       ListTopics.listerTopic().stream()
               .forEach(System.out::println);
       var greenTopology = GreetingsTopology.buildTopology();
       var kafkaStream = new KafkaStreams(greenTopology,properties);

       Runtime.getRuntime().addShutdownHook(new Thread(kafkaStream::close));

       try {
           kafkaStream.start();
       }catch (Exception e){
           log.error("kafka stream not starting : {} ",e.getMessage());
       }
    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
           createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
