package sandbox.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Yelling Kafka Streams application
 *
 * As described in Kafka Streams in Action (https://www.manning.com/books/kafka-streams-in-action)
 */
public class YellingApp extends KafkaStreamsApplication {

  public YellingApp() {
    super("yelling-app");
  }

  @Override
  protected Topology buildTopology() {
    Serde<String> serde = Serdes.String();
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("src-topic", Consumed.with(serde, serde))  // add source node
        .mapValues(i -> i.toUpperCase())                      // add processor node
        .to("out-topic", Produced.with(serde, serde));        // add sink node

    return builder.build();
  }

  public static void main(String... args) {
    YellingApp app = new YellingApp();
    app.run();
  }
}
