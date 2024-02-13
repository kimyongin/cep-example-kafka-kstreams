package org.example.service.count.simple;

import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCountDSLDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();

  public WordCountDSLDemo(StreamsBuilder streamsBuilder) {
    KStream<String, String> messageStream = streamsBuilder
        .stream("streams-app-dsl-input", Consumed.with(STRING_SERDE, STRING_SERDE));

    messageStream
        .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .groupBy((key, word) -> word, Grouped.with("dsl-repartition", STRING_SERDE, STRING_SERDE))
        .aggregate(
            () -> 0L,
            (key, value, aggregate) -> aggregate + 1,
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("dsl-count").withValueSerde(LONG_SERDE)
        )
        .filter((key, count) -> count > 50)
        .toStream().to("streams-app-dsl-output");
  }

}