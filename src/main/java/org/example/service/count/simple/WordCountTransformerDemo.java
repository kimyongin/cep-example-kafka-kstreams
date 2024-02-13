package org.example.service.count.simple;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.service.count.window.WordCountWindowProcessorDemo.FilterProcessor;
import org.example.service.count.window.WordCountWindowProcessorDemo.WordSplitProcessor;

public class WordCountTransformerDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();

  public WordCountTransformerDemo(StreamsBuilder streamsBuilder) {
    streamsBuilder.<String, String>stream("streams-app-transformer-input")
        .process(WordSplitProcessor::new)
        .groupBy((key, word) -> word, Grouped.with("transformer-repartition", STRING_SERDE, STRING_SERDE))
        .aggregate(
            () -> 0L,
            (key, value, aggregate) -> aggregate + 1,
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("transformer-count").withValueSerde(LONG_SERDE)
        )
        .toStream()
        .process(FilterProcessor::new)
        .to("streams-app-transformer-output", Produced.with(STRING_SERDE, LONG_SERDE));
  }
}