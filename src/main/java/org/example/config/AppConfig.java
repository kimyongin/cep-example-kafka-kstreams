package org.example.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.example.service.count.WordCountDSLDemo;
import org.example.service.count.WordCountProcessorDemo;
import org.example.service.count.WordCountTransformerDemo;
import org.example.service.count_window.WordCountWindowDSLDemo;
import org.example.service.count_window.WordCountWindowProcessorDemo;
import org.example.service.count_window.WordCountWindowTransformerDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

//  @Autowired
  void buildPipeLine1(StreamsBuilder streamsBuilder) {
    new WordCountDSLDemo(streamsBuilder);
    new WordCountProcessorDemo(streamsBuilder);
    new WordCountTransformerDemo(streamsBuilder);
  }

  @Autowired
  void buildPipeLine2(StreamsBuilder streamsBuilder) {
    new WordCountWindowDSLDemo(streamsBuilder);
    new WordCountWindowProcessorDemo(streamsBuilder);
    new WordCountWindowTransformerDemo(streamsBuilder);
  }

}
