package sandbox.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import sandbox.kafka.streams.util.KafkaAdmin;

/**
 * Yelling Kafka Streams application
 *
 * As described in Kafka Streams in Action (https://www.manning.com/books/kafka-streams-in-action)
 */
public class YellingApp extends KafkaStreamsApplication {

  public YellingApp() {
    super("YellingApp");
  }

  @Override
  protected Topology buildTopology() {
    Serde<String> serde = Serdes.String();
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("src-topic", Consumed.with(serde, serde))            // add source node
        .mapValues(i -> i.toUpperCase())                                // add processor node
        .print(Printed.<String, String>toSysOut().withLabel("output")); // add sink node

    return builder.build();
  }

  public static void main(String... args) {
    // create input topic
    KafkaAdmin admin = new KafkaAdmin("localhost:9092");
    admin.createTopic("src-topic", 1, 1);
    admin.close();

    // start app
    YellingApp app = new YellingApp();
    app.run();
  }
}
