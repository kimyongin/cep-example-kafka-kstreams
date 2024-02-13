package org.example.service.usecase1.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GcBasicChallengeEvents {

  private String guid;
  private Long level;
  private Long time;

  public static Serde<GcBasicChallengeEvents> SERDE = Serdes.serdeFrom(new GcBasicChallengeEventsSerializer(), new GcBasicChallengeEventsDeserializer());

  public static class GcBasicChallengeEventsSerializer implements Serializer<GcBasicChallengeEvents> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, GcBasicChallengeEvents data) {
      try {
        String json = objectMapper.writeValueAsString(data);
        return json.getBytes(StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class GcBasicChallengeEventsDeserializer implements Deserializer<GcBasicChallengeEvents> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public GcBasicChallengeEvents deserialize(String topic, byte[] data) {
      try {
        String json = new String(data, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, GcBasicChallengeEvents.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
