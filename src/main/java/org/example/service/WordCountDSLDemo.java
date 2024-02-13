package org.example.service;

import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WordCountDSLDemo {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
            .stream("streams-app-dsl-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        messageStream
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, word) -> word, Grouped.with("dsl-repartition", STRING_SERDE, STRING_SERDE))
            .count(Materialized.as("dsl-count"))
            .filter((key, count) -> count > 50)
            .toStream().to("streams-app-dsl-output");
    }

}