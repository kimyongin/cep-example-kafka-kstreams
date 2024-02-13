package org.example.service.count.simple;

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

public class WordCountProcessorDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();

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

      String word = record.value();
      final Long oldValue = kvStore.get(word);
      Long newValue;

      if (oldValue == null) {
        kvStore.put(word, 1L);
        newValue = 1L;
      } else {
        kvStore.put(word, oldValue + 1);
        newValue = oldValue + 1L;
      }
      context.forward(new Record<>(word, newValue, record.timestamp()));
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

  public WordCountProcessorDemo(StreamsBuilder streamsBuilder) {
    StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("processor-count"), STRING_SERDE, LONG_SERDE);

    Topology topology = streamsBuilder.build();
    topology.addSource("source", "streams-app-processor-input");
    topology.addProcessor("split", WordSplitProcessor::new, "source");
    topology.addSink("repartition", "streams-app-processor-repartition", STRING_SERDE.serializer(), STRING_SERDE.serializer(), "split");

    topology.addSource("repartition-source", STRING_SERDE.deserializer(), STRING_SERDE.deserializer(), "streams-app-processor-repartition");
    topology.addProcessor("count", () -> new WordCountProcessor("processor-count"), "repartition-source");
    topology.addStateStore(storeBuilder, "count");
    topology.addProcessor("filter", FilterProcessor::new, "count");
    topology.addSink("sink", "streams-app-processor-output", STRING_SERDE.serializer(), LONG_SERDE.serializer(), "filter");
  }
}