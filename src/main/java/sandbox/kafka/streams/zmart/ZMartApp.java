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
import sandbox.kafka.streams.util.KafkaAdmin;

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
    Serde<Long> longSerde = Serdes.Long();
    Serde<String> stringSerde = Serdes.String();
    Serde<Purchase> purchaseSerde = JsonSerde.of(Purchase.class);
    Serde<PurchasePattern> purchasePatternSerde = JsonSerde.of(PurchasePattern.class);
    Serde<Rewards> rewardsSerde = JsonSerde.of(Rewards.class);

    // topology builder
    StreamsBuilder builder = new StreamsBuilder();

    // create stream of transactions, with masked credit card numbers
    KStream<String, Purchase> transactions = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
        .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

    // create stream of filtered transactions, keyed by purchase date
    KStream<Long, Purchase> purchases = transactions.filter((key, purchase) -> purchase.getPrice() > 5.00)
        .selectKey((key, purchase) -> purchase.getPurchaseDate().getTime());
    purchases.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
    purchases.to("purchases", Produced.with(longSerde, purchaseSerde));

    // create stream of purchase patterns
    KStream<String, PurchasePattern> patterns = transactions.mapValues(p -> PurchasePattern.builder(p).build());
    patterns.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
    patterns.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

    // create stream of reward data
    KStream<String, Rewards> rewards = transactions.mapValues(p -> Rewards.builder(p).build());
    rewards.print(Printed.<String, Rewards>toSysOut().withLabel("rewards"));
    rewards.to("rewards", Produced.with(stringSerde, rewardsSerde));

    return builder.build();
  }

  public static void main(String... args) {
    // create input topic
    KafkaAdmin admin = new KafkaAdmin("localhost:9092");
    admin.createTopic("transactions", 1, 1);
    admin.close();

    // start app
    ZMartApp app = new ZMartApp();
    app.run();
  }
}
