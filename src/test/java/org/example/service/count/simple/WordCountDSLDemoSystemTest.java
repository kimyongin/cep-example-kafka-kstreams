package org.example.service.count.simple;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
public class WordCountDSLDemoSystemTest {

    private static KafkaContainer kafkaContainer;
    private KafkaStreams kafkaStreams;

    @Autowired
    KafkaStreamsConfiguration kafkaConfig;

    @BeforeAll
    static void startKafkaContainer() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));
        kafkaContainer.start();

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(Arrays.asList(
                    new NewTopic("streams-app-processor-input", 3, (short) 1),
                    new NewTopic("streams-app-processor-repartition", 3, (short) 1)))
                .all().get();
        } catch (Exception e) {
            throw new RuntimeException("토픽 생성 중 오류 발생", e);
        }
    }

    @AfterAll
    static void stopKafkaContainer() {
        kafkaContainer.stop();
    }

    @BeforeEach
    public void setup() {
        // Kafka Streams 구성 설정
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = builder.build();
        new WordCountProcessorDemo(topology);

        kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
        Awaitility.await().atMost(Duration.ofSeconds(100))
            .until(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING);
    }

    @AfterEach
    public void tearDown() {
        kafkaStreams.close();
        kafkaContainer.stop();
    }

    @Test
    public void testOneWord() throws ExecutionException, InterruptedException {
        // 테스트용 Producer 생성
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // 테스트용 Consumer 생성
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("streams-app-processor-output"));

        // 입력 토픽에 메시지 전송
        producer.send(new ProducerRecord<>("streams-app-processor-input", 0, null, "A A A A A A A A A A")).get();
        producer.send(new ProducerRecord<>("streams-app-processor-input", 1, null, "A A A A A A A A A A")).get();
        producer.send(new ProducerRecord<>("streams-app-processor-input", 2, null, "A A A A A A A A A A")).get();
        producer.send(new ProducerRecord<>("streams-app-processor-input", 0, null, "A A A A A A A A A A")).get();
        producer.send(new ProducerRecord<>("streams-app-processor-input", 1, null, "A A A A A A A A A A")).get();
        producer.send(new ProducerRecord<>("streams-app-processor-input", 2, null, "A")).get();

        // 출력 토픽에서 결과 확인
        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(100));
        assertThat(records.count(), equalTo(1));
        assertThat(records.iterator().next().value(), equalTo(51L));

        ReadOnlyKeyValueStore<String, Long> keyValueStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("processor-count", QueryableStoreTypes.keyValueStore()));
        assertThat(keyValueStore.get("a"), equalTo(51L));

        producer.close();
        consumer.close();
    }
}
