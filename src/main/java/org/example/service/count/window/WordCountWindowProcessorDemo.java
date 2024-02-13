package org.example.service.count.window;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class WordCountWindowProcessorDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());


  public static class WordSplitProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;

    @Override
    public void init(final ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(final Record<String, String> record) {
      final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");
      for (String word : words) {
        context.forward(new Record<>(word, word, record.timestamp()));
      }
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }

  public static class WordCountProcessor implements Processor<String, String, String, Long> {

    private KeyValueStore<String, Long> kvStore;
    private ProcessorContext<String, Long> context;
    private final String storeName;

    public WordCountProcessor(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(final ProcessorContext<String, Long> context) {
      this.context = context;
      kvStore = context.getStateStore(storeName);
    }

    @Override
    public void process(final Record<String, String> record) {

      String key = record.key();
      final Long oldValue = kvStore.get(key);
      Long newValue;

      if (oldValue == null) {
        kvStore.put(key, 1L);
        newValue = 1L;
      } else {
        kvStore.put(key, oldValue + 1);
        newValue = oldValue + 1L;
      }
      context.forward(new Record<>(key, newValue, record.timestamp()));
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }

  public static class FilterProcessor implements Processor<String, Long, String, Long> {

    private ProcessorContext<String, Long> context;

    @Override
    public void init(final ProcessorContext<String, Long> context) {
      this.context = context;
    }

    @Override
    public void process(final Record<String, Long> record) {
      if (record.value() > 50) {
        context.forward(record);
      }
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }

  public static class TimeWindowProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    private final Duration windowSize;

    public TimeWindowProcessor(Duration windowSize) {
      this.windowSize = windowSize;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
      long now = record.timestamp();
      long windowStart = now - (now % windowSize.toMillis());
      long windowEnd = windowStart + windowSize.toMillis();
      String windowedKey = record.key() + "@" + windowStart + "@" + windowEnd;
      context.forward(new Record<>(windowedKey, record.value(), record.timestamp()));
    }

    @Override
    public void close() {
    }
  }

  public static class MapProcessor implements Processor<String, Long, String, String> {

    private ProcessorContext<String, String> context;

    @Override
    public void init(final ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(final Record<String, Long> record) {
      String[] parts = record.key().split("@");
      String key = parts[0];
      long start = Long.parseLong(parts[1]);
      long end = Long.parseLong(parts[2]);
      String value = String.format("{\"key\": \"%s\", \"start\": %s, \"end\": %s, \"count\": %d}",
          key, formatter.format(Instant.ofEpochMilli(start)), formatter.format(Instant.ofEpochMilli(end)), record.value());
      context.forward(new Record<>(null, value, record.timestamp()));
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }

  public WordCountWindowProcessorDemo(StreamsBuilder streamsBuilder) {
    StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("processor-count-windowed"), STRING_SERDE, LONG_SERDE);

    Topology topology = streamsBuilder.build();
    topology.addSource("source", "streams-app-processor-input");
    topology.addProcessor("split", WordSplitProcessor::new, "source");
    topology.addSink("repartition", "streams-app-processor-repartition", STRING_SERDE.serializer(), STRING_SERDE.serializer(), "split");

    topology.addSource("repartition-source", STRING_SERDE.deserializer(), STRING_SERDE.deserializer(), "streams-app-processor-repartition");
    topology.addProcessor("window", () -> new TimeWindowProcessor(Duration.ofSeconds(5)), "repartition-source");
    topology.addProcessor("count", () -> new WordCountProcessor("processor-count-windowed"), "window");
    topology.addStateStore(storeBuilder, "count");
    topology.addProcessor("filter", FilterProcessor::new, "count");
    topology.addProcessor("map", MapProcessor::new, "filter");
    topology.addSink("sink", "streams-app-processor-output", STRING_SERDE.serializer(), STRING_SERDE.serializer(), "map");
  }
}