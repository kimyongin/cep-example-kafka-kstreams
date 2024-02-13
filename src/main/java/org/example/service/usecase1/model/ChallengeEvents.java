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
public class ChallengeEvents {

  private String guid;
  private Long time;

  public static Serde<ChallengeEvents> SERDE = Serdes.serdeFrom(new ChallengeEventsSerializer(), new ChallengeEventsDeserializer());

  public static class ChallengeEventsSerializer implements Serializer<ChallengeEvents> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, ChallengeEvents data) {
      try {
        String json = objectMapper.writeValueAsString(data);
        return json.getBytes(StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class ChallengeEventsDeserializer implements Deserializer<ChallengeEvents> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ChallengeEvents deserialize(String topic, byte[] data) {
      try {
        String json = new String(data, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, ChallengeEvents.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
