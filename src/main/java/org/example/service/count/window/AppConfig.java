//package org.example.service.count.window;
//
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.TopicBuilder;
//
//@Configuration
//public class AppConfig {
//
//  @Bean
//  NewTopic DslInputTopic() {
//    return TopicBuilder.name("streams-app-dsl-input")
//        .partitions(3)
//        .replicas(2)
//        .build();
//  }
//
//  @Bean
//  NewTopic ProcessorInputTopic() {
//    return TopicBuilder.name("streams-app-processor-input")
//        .partitions(3)
//        .replicas(2)
//        .build();
//  }
//
//  @Bean
//  NewTopic ProcessorOutputTopic() {
//    return TopicBuilder.name("streams-app-processor-repartition")
//        .partitions(3)
//        .replicas(2)
//        .build();
//  }
//
//  @Bean
//  NewTopic TransformerInputTopic() {
//    return TopicBuilder.name("streams-app-transformer-input")
//        .partitions(3)
//        .replicas(2)
//        .build();
//  }
//
//  @Autowired
//  void buildPipeLine(StreamsBuilder streamsBuilder) {
//    new WordCountWindowDSLDemo(streamsBuilder);
//    new WordCountWindowProcessorDemo(streamsBuilder);
//    new WordCountWindowTransformerDemo(streamsBuilder);
//  }
//}
