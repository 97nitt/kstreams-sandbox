package sandbox.kafka.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class that includes some boilerplate code for Kafka Streams applications so concrete
 * implementations can focus solely on building a topology.
 *
 */
public abstract class KafkaStreamsApplication {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);

  private final String id;

  /**
   * Constructor.
   *
   * @param id Kafka Streams application id
   */
  public KafkaStreamsApplication(String id) {
    this.id = id;
  }

  /**
   * Run this Kafka Streams application.
   */
  public void run() {
    // create KafkaStreams client
    Topology topology = buildTopology();
    logger.info("Creating KafkaStreams client with {}", topology.describe());

    Properties config = getConfig();
    logger.info("Creating KafkaStreams client with config: {}", config);

    KafkaStreams client = new KafkaStreams(topology, config);

    // register a JVM shutdown hook that will close the KafkaStreams client
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
      @Override
      public void run() {
        logger.info("Closing KafkaStreams client");
        client.close();
        latch.countDown();
      }
    });

    // start KafkaStreams client
    logger.info("Starting KafkaStreams client");
    client.start();

    // wait for shutdown hook to be called
    try {
      latch.await();
    } catch (InterruptedException e) {
      // meh...
    }
  }

  /**
   * Build Kafka Streams {@link Topology}. To be implemented by concrete subclasses.
   *
   * @return topology
   */
  protected abstract Topology buildTopology();

  /**
   * Get Kafka Streams client configuration.
   *
   * @return client configuration
   */
  private Properties getConfig() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, id);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }
}
