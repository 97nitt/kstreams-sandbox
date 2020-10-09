package sandbox.kafka.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YellingApp {

  private static final Logger logger = LoggerFactory.getLogger(YellingApp.class);

  public static void main(String... args) {
    // create Serde (both key & value will be Strings)
    Serde<String> serde = Serdes.String();

    // build topology
    Topology topology = buildTopology(serde);

    // get configuration
    Properties config = getConfig();

    // create Kafka Streams client
    KafkaStreams streams = new KafkaStreams(topology, config);

    // register a JVM shutdown hook that will close the Kafka Streams client
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
      @Override
      public void run() {
        logger.info("Closing KafkaStreams client");
        streams.close();
        latch.countDown();
      }
    });

    // start Kafka Streams client
    logger.info("Starting KafkaStreams client");
    streams.start();

    // wait for shutdown hook to be called
    try {
      latch.await();
    } catch (InterruptedException e) {
      // meh...
    }
  }

  private static Properties getConfig() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }

  private static Topology buildTopology(Serde<String> serde) {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("src-topic", Consumed.with(serde, serde))  // add source node
        .mapValues(i -> i.toUpperCase())                      // add processor node
        .to("out-topic", Produced.with(serde, serde));        // add sink node

    return builder.build();
  }
}
