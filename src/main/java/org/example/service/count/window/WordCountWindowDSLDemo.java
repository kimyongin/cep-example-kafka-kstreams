package org.example.service.count.window;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

public class WordCountWindowDSLDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

  public WordCountWindowDSLDemo(StreamsBuilder streamsBuilder) {
    KStream<String, String> messageStream = streamsBuilder
        .stream("streams-app-dsl-input", Consumed.with(STRING_SERDE, STRING_SERDE));

    messageStream
        .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .groupBy((key, word) -> word, Grouped.with("dsl-repartition", STRING_SERDE, STRING_SERDE))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
        .aggregate(
            () -> 0L,
            (key, value, aggregate) -> aggregate + 1,
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("dsl-count-windowed").withValueSerde(LONG_SERDE)
        )
        .filter((key, count) -> {
          return count > 50;
        })
        .toStream()
        .map((stringWindowed, count) -> {
          String key = stringWindowed.key();
          long start = stringWindowed.window().start();
          long end = stringWindowed.window().end();
          String value = String.format("{\"key\": \"%s\", \"start\": %s, \"end\": %s, \"count\": %d}",
              key, formatter.format(Instant.ofEpochMilli(start)), formatter.format(Instant.ofEpochMilli(end)), count);
          return new KeyValue<>(null, value);
        })
        .to("streams-app-dsl-output");
  }

}

