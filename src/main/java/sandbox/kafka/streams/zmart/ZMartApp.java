package sandbox.kafka.streams.zmart;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;

/**
 * ZMart Kafka Streams application
 *
 * As described in Kafka Streams in Action (https://www.manning.com/books/kafka-streams-in-action)
 */
public class ZMartApp extends KafkaStreamsApplication {

  public ZMartApp() {
    super("ZMart");
  }

  @Override
  protected Topology buildTopology() {
    // create serializers/deserializers
    Serde<String> stringSerde = Serdes.String();
    Serde<Purchase> purchaseSerde = JsonSerde.of(Purchase.class);
    Serde<PurchasePattern> purchasePatternSerde = JsonSerde.of(PurchasePattern.class);
    Serde<Rewards> rewardsSerde = JsonSerde.of(Rewards.class);

    // topology builder
    StreamsBuilder builder = new StreamsBuilder();

    // create stream of purchases, with masked credit card numbers
    KStream<String, Purchase> purchases = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
        .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
    purchases.print(Printed.<String, Purchase>toSysOut().withLabel("purchases"));
    purchases.to("purchases", Produced.with(stringSerde, purchaseSerde));

    // create stream of purchase patterns
    KStream<String, PurchasePattern> patterns = purchases.mapValues(p -> PurchasePattern.builder(p).build());
    patterns.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
    patterns.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

    // create stream of reward data
    KStream<String, Rewards> rewards = purchases.mapValues(p -> Rewards.builder(p).build());
    rewards.print(Printed.<String, Rewards>toSysOut().withLabel("rewards"));
    rewards.to("rewards", Produced.with(stringSerde, rewardsSerde));

    return builder.build();
  }

  public static void main(String... args) {
    ZMartApp app = new ZMartApp();
    app.run();
  }
}
