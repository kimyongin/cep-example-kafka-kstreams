package org.example.config;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

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

//  @Bean
//  void clearTopic() {
//    Properties config = new Properties();
//    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress); // Kafka 브로커 주소 설정
//
//    try (AdminClient admin = AdminClient.create(config)) {
//      ListTopicsResult listTopics = admin.listTopics();
//      Set<String> topicNames = listTopics.names().get();
//
//      DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(topicNames);
//      deleteTopicsResult.all().get();
//
//      System.out.println("All topics deleted successfully");
//    } catch (InterruptedException | ExecutionException e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Bean
//  void clearState() {
//    Path path = Paths.get(stateStoreLocation);
//
//    try {
//      deleteDirectoryRecursively(path);
//      System.out.println("State directory deleted successfully.");
//    } catch (IOException e) {
//      e.printStackTrace();
//      System.err.println("Error deleting state directory.");
//    }
//  }
//
//  public static void deleteDirectoryRecursively(Path path) throws IOException {
//    if (Files.isDirectory(path)) {
//      // 디렉터리인 경우, 모든 내부 파일 및 디렉터리를 재귀적으로 삭제
//      Files.list(path).forEach(p -> {
//        try {
//          deleteDirectoryRecursively(p);
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//      });
//    }
//    Files.delete(path); // 파일 또는 빈 디렉터리 삭제
//  }
}