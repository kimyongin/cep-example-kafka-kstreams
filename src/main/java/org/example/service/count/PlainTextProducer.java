package org.example.service.count;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@AllArgsConstructor
public class PlainTextProducer {

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

  @PostMapping("/message/{partition}")
  public void addMessage(@PathVariable("partition") Integer partition, @RequestBody String message) {
    sendMessage("streams-app-dsl-input", partition, message);
    sendMessage("streams-app-processor-input", partition, message);
    sendMessage("streams-app-transformer-input", partition, message);
  }
}