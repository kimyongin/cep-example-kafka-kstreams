package org.example.service.usecase1;

import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.service.usecase1.model.ChallengeEvents;
import org.example.service.usecase1.model.GcBasic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@AllArgsConstructor
public class EventProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public void sendMessage(String topic, Integer partition, String message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
        topic,       // topic
        partition,   // partition
        null,        // timestamp
        null,        // key
        message,     // value
        null         // headers
    );
    kafkaTemplate.send(producerRecord)
        .whenComplete((result, ex) -> {
          if (ex == null) {
            log.info("Message sent to topic: {}", message);
          } else {
            log.error("Failed to send message", ex);
          }
        });
  }

  @PostMapping("/gc-basic/{partition}")
  public void addGcBasicMessage(@PathVariable("partition") Integer partition, @RequestBody GcBasic gcBasic) {
    byte[] serialize = GcBasic.SERDE.serializer().serialize("streams-app-usecase1-gc-basic", gcBasic);
    sendMessage("streams-app-usecase1-gc-basic", partition, new String(serialize, StandardCharsets.UTF_8));
  }

  @PostMapping("/challenge-events/{partition}")
  public void addDslMessage(@PathVariable("partition") Integer partition, @RequestBody ChallengeEvents challengeEvents) {
    byte[] serialize = ChallengeEvents.SERDE.serializer().serialize("streams-app-usecase1-challenge-events", challengeEvents);
    sendMessage("streams-app-usecase1-challenge-events", partition, new String(serialize, StandardCharsets.UTF_8));
  }
}