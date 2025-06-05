package com.learnkafkastreams.topology;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

 public  class ListTopics {
    public static List<String> listerTopic(){
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (
                AdminClient adminClient = AdminClient.create(config)) {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.stream().toList();

        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la récupération des topics : " + e.getMessage());
            e.printStackTrace();
        }

        return List.of();
    }
}
