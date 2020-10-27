package sandbox.kafka.streams.trading;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;
import sandbox.kafka.streams.util.KafkaAdmin;

@SuppressWarnings({"rawtypes", "unchecked"})
public class StockTradingApp extends KafkaStreamsApplication {

  private static final Logger logger = LoggerFactory.getLogger(StockTradingApp.class);

  private final Serde<Long> longSerde = Serdes.Long();
  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<StockTransaction> transactionSerde = JsonSerde.of(StockTransaction.class);
  private final Serde<StockTransactionKey> transactionKeySerde = JsonSerde.of(StockTransactionKey.class);
  private final Serde<ShareVolume> shareVolumeSerde = JsonSerde.of(ShareVolume.class);
  private final Serde<TopNList> topNListSerde = JsonSerde.of(TopNList.class);

  public StockTradingApp() {
    super("StockTradingApp");
  }

  @Override
  protected Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // create stream of stock transactions
    KStream<String, StockTransaction> transactions = builder.stream(
        "transactions",
        Consumed.with(stringSerde, transactionSerde).withName("SourceStockTransactions"));

    // create KTable of accumulated share volumes
    top5ShareVolumes(transactions);

    // create windowed KTable of transaction counts
    windowedCounts(transactions);

    return builder.build();
  }

  private void top5ShareVolumes(KStream<String, StockTransaction> transactions) {
    // state stores
    String volumeStoreName = "ShareVolumeStore";
    KeyValueBytesStoreSupplier volumeStoreSupplier = Stores.inMemoryKeyValueStore(volumeStoreName);

    String topNStoreName = "TopNStore";
    KeyValueBytesStoreSupplier topNStoreSupplier = Stores.inMemoryKeyValueStore(topNStoreName);

    // build out topology
    transactions
        // key by stock symbol
        .selectKey(
            (key, shareVolume) -> shareVolume.getSymbol(),
            Named.as("KeyByStockSymbol"))
        // map StockTransaction to ShareVolume
        .mapValues(
            transaction -> ShareVolume.builder(transaction).build(),
            Named.as("MapToShareVolume"))
        // group by stock symbol
        .groupByKey(
            Grouped.with(stringSerde, shareVolumeSerde)
                .withName("GroupByStockSymbol"))
        // accumulate share volumes per stock symbol
        .reduce(
            (left, right) -> ShareVolume.builder(left, right).build(),
            Named.as("AccumulateShareVolume"),
            Materialized.<String, ShareVolume>as(volumeStoreSupplier)
                .withKeySerde(stringSerde)
                .withValueSerde(shareVolumeSerde))
        // group share volumes by industry
        .groupBy(
            (key, shareVolume) -> KeyValue.pair(shareVolume.getIndustry(), shareVolume),
            Grouped.with(stringSerde, shareVolumeSerde)
                .withName("GroupByIndustry"))
        // aggregate share volumes by industry into TopNList
        .aggregate(
            () -> new TopNList<ShareVolume>((left, right) -> right.getShares() - left.getShares(), 5),
            (industry, shareVolume, topN) -> topN.add(shareVolume),
            (industry, shareVolume, topN) -> topN.remove(shareVolume),
            Named.as("AggregateTopNList"),
            Materialized.<String, TopNList>as(topNStoreSupplier)
                .withKeySerde(stringSerde)
                .withValueSerde(topNListSerde))
        // map TopNList to a String
        .mapValues((industry, topN) -> topN.toString(), Named.as("MapTopNListToString"))
        // convert KTable to KStream
        .toStream(Named.as("ConvertToStream"))
        // log each result
        //.peek((industry, topN) -> logger.info("Top N List for industry {}: {}", industry, topN), Named.as("LogResult"))
        // write to output topic
        .to("topn-list", Produced.with(stringSerde, stringSerde).withName("TopNListSink"));
  }

  private void windowedCounts(KStream<String, StockTransaction> transactions) {
    // group transactions by key (customer id + stock symbol)
    KGroupedStream<StockTransactionKey, StockTransaction> transactionsByKey = transactions
        // key by transaction key
        .selectKey(
            (key, transaction) -> StockTransactionKey.builder(transaction).build(),
            Named.as("KeyByTransactionKey"))
        // group by transaction key
        .groupByKey(
          Grouped.with(transactionKeySerde, transactionSerde)
              .withName("GroupByTransactionKey"));

    // aggregate transaction counts using session windows
    sessionWindowedCounts(transactionsByKey);

    // aggregate transaction counts using tumbling windows
    tumblingWindowedCounts(transactionsByKey);

    // aggregate transaction counts using hopping windows
    hoppingWindowedCounts(transactionsByKey);
  }

  private void sessionWindowedCounts(KGroupedStream<StockTransactionKey, StockTransaction> transactionsByKey) {
    // session window state store
    SessionBytesStoreSupplier storeSupplier = Stores.inMemorySessionStore(
        "SessionWindowTransactionCountStore",
        Duration.ofMinutes(15));

    transactionsByKey
        // use 20s session windows
        .windowedBy(SessionWindows.with(Duration.ofSeconds(20)))
        // aggregate counts
        .count(Named.as("SessionWindowTransactionCount"), Materialized.<StockTransactionKey, Long>as(storeSupplier)
            .withKeySerde(transactionKeySerde)
            .withValueSerde(longSerde))
        // convert KTable to KStream
        .toStream(Named.as("SessionWindowTransactionCountsToStream"))
        // print results to console
        .print(Printed.<Windowed<StockTransactionKey>, Long>toSysOut().withLabel("transaction-counts (session window)"));
  }

  private void tumblingWindowedCounts(KGroupedStream<StockTransactionKey, StockTransaction> transactionsByKey) {
    WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore(
        "TumblingWindowTransactionCountStore",
        Duration.ofSeconds(60),   // retention = window size
        Duration.ofSeconds(60),   // window size
        false);

    transactionsByKey
        // use 60s tumbling windows
        .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
        // aggregate counts
        .count(Named.as("TumblingWindowTransactionCount"), Materialized.<StockTransactionKey, Long>as(storeSupplier)
            .withKeySerde(transactionKeySerde)
            .withValueSerde(longSerde))
        // convert KTable to KStream
        .toStream(Named.as("TumblingWindowTransactionCountToStream"))
        // print results to console
        .print(Printed.<Windowed<StockTransactionKey>, Long>toSysOut().withLabel("transaction-counts (tumbling window)"));
  }

  private void hoppingWindowedCounts(KGroupedStream<StockTransactionKey, StockTransaction> transactionsByKey) {
    WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore(
        "HoppingWindowTransactionCountStore",
        Duration.ofSeconds(60),   // retention = window size
        Duration.ofSeconds(60),   // window size
        false);

    transactionsByKey
        // use 60s hopping windows every 10s
        .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).advanceBy(Duration.ofSeconds(10)))
        // aggregate counts
        .count(Named.as("HoppingWindowTransactionCount"), Materialized.<StockTransactionKey, Long>as(storeSupplier)
            .withKeySerde(transactionKeySerde)
            .withValueSerde(longSerde))
        // convert KTable to KStream
        .toStream(Named.as("HoppingWindowTransactionCountToStream"))
        // print results to console
        .print(Printed.<Windowed<StockTransactionKey>, Long>toSysOut().withLabel("transaction-counts (hopping window)"));
  }

  public static void main(String... args) {
    // create input topic
    KafkaAdmin admin = new KafkaAdmin("localhost:9092");
    admin.createTopic("transactions", 1, 1);
    admin.close();

    // start app
    StockTradingApp app = new StockTradingApp();
    app.run();
  }
}
