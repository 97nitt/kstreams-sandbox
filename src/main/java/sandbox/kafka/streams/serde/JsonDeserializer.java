package sandbox.kafka.streams.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Generic JSON deserializer
 */
public class JsonDeserializer<T> implements Deserializer<T> {

  private final Gson gson;
  private final Class<T> type;

  public JsonDeserializer(Gson gson, Class<T> type) {
    this.gson = gson;
    this.type = type;
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return gson.fromJson(new String(bytes), type);
  }
}
