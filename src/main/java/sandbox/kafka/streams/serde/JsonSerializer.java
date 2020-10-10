package sandbox.kafka.streams.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic JSON serializer
 */
public class JsonSerializer<T> implements Serializer<T> {

  private final Gson gson;

  public JsonSerializer(Gson gson) {
    this.gson = gson;
  }

  @Override
  public byte[] serialize(String topic, T t) {
    return gson.toJson(t).getBytes();
  }
}
