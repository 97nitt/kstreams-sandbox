package sandbox.kafka.streams.serde;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

/**
 * Generic JSON serializer/deserializer
 */
public class JsonSerde<T> extends WrapperSerde<T> {

  public static <T> JsonSerde<T> of(Class<T> type) {
    Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    return new JsonSerde<>(gson, type);
  }

  private JsonSerde(Gson gson, Class<T> type) {
    super(new JsonSerializer<>(gson), new JsonDeserializer<>(gson, type));
  }
}
