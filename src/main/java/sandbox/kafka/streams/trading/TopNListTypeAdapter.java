package sandbox.kafka.streams.trading;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Iterator;

public class TopNListTypeAdapter extends TypeAdapter<TopNList<ShareVolume>> {

  private final Gson gson = new Gson();

  @Override
  public void write(JsonWriter writer, TopNList<ShareVolume> value) throws IOException {

    if (value == null) {
      writer.nullValue();
      return;
    }

    Iterator<ShareVolume> iterator = value.iterator();
    writer.beginArray();
    while (iterator.hasNext()) {
      ShareVolume shareVolume = iterator.next();
      if (shareVolume != null) {
        writer.value(gson.toJson(shareVolume));
      }
    }
    writer.endArray();
  }

  @Override
  public TopNList<ShareVolume> read(JsonReader reader) throws IOException {
    TopNList<ShareVolume> list = new TopNList<>((left, right) -> right.getShares() - left.getShares(), 5);
    reader.beginArray();
    while (reader.hasNext()) {
      list.add(gson.fromJson(reader.nextString(), ShareVolume.class));
    }
    reader.endArray();
    return list;
  }
}
