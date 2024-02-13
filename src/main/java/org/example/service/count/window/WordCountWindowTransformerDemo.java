package org.example.service.count.window;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.example.service.count.window.WordCountWindowProcessorDemo.WordSplitProcessor;

public class WordCountWindowTransformerDemo {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

  public static class FilterProcessor implements Processor<Windowed<String>, Long, Windowed<String>, Long> {
    private ProcessorContext<Windowed<String>, Long> context;

    @Override
    public void init(final ProcessorContext<Windowed<String>, Long> context) {
      this.context = context;
    }

    @Override
    public void process(Record<Windowed<String>, Long> record) {
      if (record.value() > 50) {
        context.forward(record);
      }
    }
  }

  public WordCountWindowTransformerDemo(StreamsBuilder streamsBuilder) {
    streamsBuilder.<String, String>stream("streams-app-transformer-input")
        .process(WordSplitProcessor::new)
        .groupBy((key, word) -> word, Grouped.with("transformer-repartition", STRING_SERDE, STRING_SERDE))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
        .aggregate(
            () -> 0L,
            (key, value, aggregate) -> aggregate + 1,
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("transformer-count-windowed").withValueSerde(LONG_SERDE)
        )
        .toStream()
        .process(FilterProcessor::new)
        .map((stringWindowed, count) -> {
          String key = stringWindowed.key();
          long start = stringWindowed.window().start();
          long end = stringWindowed.window().end();
          String value = String.format("{\"key\": \"%s\", \"start\": %s, \"end\": %s, \"count\": %d}",
              key, formatter.format(Instant.ofEpochMilli(start)), formatter.format(Instant.ofEpochMilli(end)), count);
          return new KeyValue<>((String)null, value);
        })
        .to("streams-app-transformer-output", Produced.with(STRING_SERDE, STRING_SERDE));
  }
}