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
public class GcBasic {

  private String guid;
  private Long level;

  public static Serde<GcBasic> SERDE = Serdes.serdeFrom(new GcBasicSerializer(), new GcBasicDeserializer());
  public static class GcBasicSerializer implements Serializer<GcBasic> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, GcBasic data) {
      try {
        String json = objectMapper.writeValueAsString(data);
        return json.getBytes(StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class GcBasicDeserializer implements Deserializer<GcBasic> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public GcBasic deserialize(String topic, byte[] data) {
      try {
        String json = new String(data, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, GcBasic.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


}
