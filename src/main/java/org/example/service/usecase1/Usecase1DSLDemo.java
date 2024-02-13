package org.example.service.usecase1;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.example.service.usecase1.model.ChallengeEvents;
import org.example.service.usecase1.model.GcBasic;
import org.example.service.usecase1.model.GcBasicChallengeEvents;

public class Usecase1DSLDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

  Serde<String> stringSerde = Serdes.String();

  public Usecase1DSLDemo(StreamsBuilder streamsBuilder) {
    KTable<String, GcBasic> gcBasicTable = streamsBuilder
        .stream("streams-app-usecase1-gc-basic", Consumed.with(stringSerde, GcBasic.SERDE))
        .filter((key, gcBasic) -> gcBasic.getLevel() > 100)
        .groupBy((key, gcBasic) -> gcBasic.getGuid(), Grouped.with("usecase1-gc-basic-repartition", STRING_SERDE, GcBasic.SERDE))
        .reduce((acc, gcBasic) -> gcBasic, Materialized.<String, GcBasic, KeyValueStore<Bytes, byte[]>>as("usecase1-gc-basic-reduce").withValueSerde(GcBasic.SERDE));

    KStream<String, ChallengeEvents> challengeEventsStream = streamsBuilder
        .stream("streams-app-usecase1-challenge-events", Consumed.with(STRING_SERDE, ChallengeEvents.SERDE))
        .filter((key, challengeEvents) -> challengeEvents.getTime() < 60)
        .selectKey((key, challengeEvents) -> challengeEvents.getGuid(), Named.as("usecase1-challenge-events-repartition"));

    KStream<String, GcBasicChallengeEvents> joinedStream = challengeEventsStream.join(
        gcBasicTable,
        (challengeEvents, gcBasic) -> new GcBasicChallengeEvents(gcBasic.getGuid(), gcBasic.getLevel(), challengeEvents.getTime()),
        Joined.with(Serdes.String(), ChallengeEvents.SERDE, GcBasic.SERDE)
    );

    joinedStream
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("usecase1-join-windowed").withValueSerde(LONG_SERDE))
        .toStream()
        .map((windowedKey, count) -> {
          String key = windowedKey.key();
          long start = windowedKey.window().start();
          long end = windowedKey.window().end();
          String value = String.format("{\"key\": \"%s\", \"start\": %s, \"end\": %s, \"count\": %d}",
              key, formatter.format(Instant.ofEpochMilli(start)), formatter.format(Instant.ofEpochMilli(end)), count);
          return new KeyValue<>(key, value);
        })
        .to("streams-app-usecase1-output");
  }
}
