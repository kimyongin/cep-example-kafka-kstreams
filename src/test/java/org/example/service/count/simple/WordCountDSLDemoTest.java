package org.example.service.count.simple;

import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@SpringBootTest()
class WordCountDSLDemoTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @Autowired
  KafkaStreamsConfiguration kafkaConfig;

  @BeforeEach
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    //Create Actual Stream Processing pipeline
    new WordCountDSLDemo(builder);
    Properties properties = kafkaConfig.asProperties();
    properties.remove(STATE_DIR_CONFIG);
    testDriver = new TopologyTestDriver(builder.build(), properties);
    inputTopic = testDriver.createInputTopic("streams-app-dsl-input", new StringSerializer(), new StringSerializer());
    outputTopic = testDriver.createOutputTopic("streams-app-dsl-output", new StringDeserializer(), new LongDeserializer());
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testOneWord() {
    KeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore("dsl-count");

    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(keyValueStore.get("a"), equalTo(10L));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(keyValueStore.get("a"), equalTo(20L));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(keyValueStore.get("a"), equalTo(30L));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(keyValueStore.get("a"), equalTo(40L));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A A A A A A A A A A");
    assertThat(keyValueStore.get("a"), equalTo(50L));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("A");
    assertThat(keyValueStore.get("a"), equalTo(51L));
    assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 51L)));
    assertThat(outputTopic.isEmpty(), is(true));
  }
}