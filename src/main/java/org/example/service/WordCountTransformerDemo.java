package org.example.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.example.service.WordCountProcessorDemo.FilterProcessor;
import org.example.service.WordCountProcessorDemo.WordSplitProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WordCountTransformerDemo {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, String>stream("transformer-input")
            .process(WordSplitProcessor::new)
            .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
            .count()
            .toStream()
            .process(FilterProcessor::new)
            .to("transformer-output", Produced.with(STRING_SERDE, LONG_SERDE));
    }
}