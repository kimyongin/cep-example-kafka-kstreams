package org.example.config;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;
  @Value(value = "${spring.kafka.application-server}")
  private String applicationServerAddress;
  @Value(value = "${spring.kafka.streams.state.dir}")
  private String stateStoreLocation;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  KafkaStreamsConfiguration kStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, "streams-app");
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(APPLICATION_SERVER_CONFIG, applicationServerAddress); // for interactive query service
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // props.put(REPARTITION_PURGE_INTERVAL_MS_CONFIG, 604800000); 리파티션에 사용된 토픽을 정리하는 설정 (기본값은 30초)
    // configure the state location to allow tests to use clean state for every run
    props.put(STATE_DIR_CONFIG, stateStoreLocation);

    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  NewTopic DslInputTopic() {
    return TopicBuilder.name("streams-app-dsl-input")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Bean
  NewTopic ProcessorInputTopic() {
    return TopicBuilder.name("streams-app-processor-input")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Bean
  NewTopic ProcessorOutputTopic() {
    return TopicBuilder.name("streams-app-processor-repartition")
        .partitions(3)
        .replicas(2)
        .build();
  }

  @Bean
  NewTopic TransformerInputTopic() {
    return TopicBuilder.name("streams-app-transformer-input")
        .partitions(3)
        .replicas(2)
        .build();
  }

}