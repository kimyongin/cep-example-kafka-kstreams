package org.example.service.usecase1;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AppConfig {

  @Bean
  NewTopic Usecase1GcBasicTopic() {
    return TopicBuilder.name("streams-app-usecase1-gc-basic")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Bean
  NewTopic Usecase1ChallengeEventsTopic() {
    return TopicBuilder.name("streams-app-usecase1-challenge-events")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Autowired
  void buildPipeLine(StreamsBuilder streamsBuilder) {
    new Usecase1DSLDemo(streamsBuilder);
  }
}
