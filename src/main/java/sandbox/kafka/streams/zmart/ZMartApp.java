package sandbox.kafka.streams.zmart;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;
import sandbox.kafka.streams.util.KafkaAdmin;

/**
 * ZMart Kafka Streams application
 *
 * As described in Kafka Streams in Action (https://www.manning.com/books/kafka-streams-in-action)
 */
public class ZMartApp extends KafkaStreamsApplication {

  private static final Logger logger = LoggerFactory.getLogger(ZMartApp.class);

  public ZMartApp() {
    super("ZMart");
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // create serializers/deserializers
    Serde<Integer> integerSerde = Serdes.Integer();
    Serde<Long> longSerde = Serdes.Long();
    Serde<String> stringSerde = Serdes.String();
    Serde<Purchase> purchaseSerde = JsonSerde.of(Purchase.class);
    Serde<PurchasePattern> purchasePatternSerde = JsonSerde.of(PurchasePattern.class);
    Serde<Rewards> rewardsSerde = JsonSerde.of(Rewards.class);
    Serde<CorrelatedPurchase> correlatedPurchaseSerde = JsonSerde.of(CorrelatedPurchase.class);

    // create state stores
    String rewardsStoreName = "RewardsStore";
    KeyValueBytesStoreSupplier rewardsStoreSupplier = Stores.inMemoryKeyValueStore(rewardsStoreName);
    StoreBuilder<KeyValueStore<String, Integer>> rewardsStoreBuilder = Stores.keyValueStoreBuilder(
        rewardsStoreSupplier,
        stringSerde,
        integerSerde);
    builder.addStateStore(rewardsStoreBuilder);

    String correlatedPurchasesLeftStoreName = "CorrelatedPurchasesLeftStore";
    WindowBytesStoreSupplier correlatedPurchasesLeftStoreSupplier = Stores.inMemoryWindowStore(
        correlatedPurchasesLeftStoreName,
        Duration.ofMinutes(41), // supplier retention == join window before + after + grace period
        Duration.ofMinutes(40), // supplier window size == join window before + after
        true);

    String correlatedPurchasesRightStoreName = "CorrelatedPurchasesRightStore";
    WindowBytesStoreSupplier correlatedPurchasesRightStoreSupplier = Stores.inMemoryWindowStore(
        correlatedPurchasesRightStoreName,
        Duration.ofMinutes(41), // supplier retention == join window before + after + grace period
        Duration.ofMinutes(40), // supplier window size == join window before + after
        true);

    // create stream of purchases, with masked credit card numbers
    KStream<String, Purchase> purchases = builder.stream(
        "transactions",
        Consumed.with(stringSerde, purchaseSerde)
            .withName("SourcePurchases")
            .withTimestampExtractor(new PurchaseTimestampExtractor()))
        .mapValues(
            p -> Purchase.builder(p).maskCreditCard().build(),
            Named.as("MaskCreditCard"));

    // create stream of material purchases, keyed by purchase date
    KStream<Long, Purchase> materialPurchases = purchases.filter(
        (key, purchase) -> purchase.getPrice() > 5.00,
        Named.as("FilterMaterialPurchases"))
        .selectKey(
            (key, purchase) -> purchase.getPurchaseTime().getTime(),
            Named.as("RekeyByTime"));

    materialPurchases.to("material-purchases", Produced.with(longSerde, purchaseSerde).withName("SinkMaterialPurchases"));
    materialPurchases.print(Printed.<Long, Purchase>toSysOut().withLabel("material-purchases"));

    // create stream of purchase patterns
    KStream<String, PurchasePattern> patterns = purchases.mapValues(
        p -> PurchasePattern.builder(p).build(),
        Named.as("MapToPurchasePatterns"));

    patterns.to("patterns", Produced.with(stringSerde, purchasePatternSerde).withName("SinkPurchasePatterns"));
    patterns.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));

    // re-key purchases by customer id
    KStream<String, Purchase> purchasesKeyedByCustomerId = purchases.selectKey(
        (key, purchase) -> purchase.getCustomerId(),
        Named.as("KeyByCustomerId"));

    // create stream of rewards data
    KStream<String, Rewards> rewards = purchasesKeyedByCustomerId.transformValues(
        () -> new PurchaseRewardsTransformer(rewardsStoreName),
        Named.as("AccumulateRewards"),
        rewardsStoreName);

    rewards.to("rewards", Produced.with(stringSerde, rewardsSerde).withName("SinkRewards"));
    rewards.print(Printed.<String, Rewards>toSysOut().withLabel("rewards"));

    // create streams of coffee & electronics department purchases
    KStream<String, Purchase>[] purchasesByDept = purchasesKeyedByCustomerId.branch(
        Named.as("BranchPurchasesByDepartment"),
        (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee"),
        (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics"));

    purchasesByDept[0].to("coffee", Produced.with(stringSerde, purchaseSerde).withName("SinkCoffeePurchases"));
    purchasesByDept[0].print(Printed.<String, Purchase>toSysOut().withLabel("coffee"));

    purchasesByDept[1].to("electronics", Produced.with(stringSerde, purchaseSerde).withName("SinkElectronicsPurchases"));
    purchasesByDept[1].print(Printed.<String, Purchase>toSysOut().withLabel("electronics"));

    // join correlated coffee & electronics purchases
    KStream<String, CorrelatedPurchase> correlatedPurchases = purchasesByDept[0].join(
        purchasesByDept[1],
        (left, right) -> CorrelatedPurchase.builder(left, right).build(),
        JoinWindows.of(Duration.ofMinutes(20)).grace(Duration.ofMinutes(1)),
        StreamJoined.with(stringSerde, purchaseSerde, purchaseSerde)
            .withName("JoinCorrelatedPurchases")
            .withThisStoreSupplier(correlatedPurchasesLeftStoreSupplier)
            .withOtherStoreSupplier(correlatedPurchasesRightStoreSupplier));

    correlatedPurchases.to("correlated-purchases", Produced.with(stringSerde, correlatedPurchaseSerde).withName("SinkCorrelatedPurchases"));
    correlatedPurchases.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("correlated-purchases"));

    // create stream of purchases entered by a specific employee suspected of fraud (tsk tsk)
    KStream<String, Purchase> suspectedFraudulentPurchases = purchases.filter(
        (key, purchase) -> purchase.getEmployeeId().equals("123456"),
        Named.as("FilterSuspectedFraudulentPurchases"));
    suspectedFraudulentPurchases.foreach((key, purchase) -> {
      logger.info("Suspected fraudulent transaction. Don't put this in Kafka but save it off somewhere else... {}", purchase);
    }, Named.as("LogSuspectedFraudulentPurchases"));

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
