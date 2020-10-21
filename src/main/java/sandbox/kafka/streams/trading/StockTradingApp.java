package sandbox.kafka.streams.trading;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;
import sandbox.kafka.streams.util.KafkaAdmin;

public class StockTradingApp extends KafkaStreamsApplication {

  private static final Logger logger = LoggerFactory.getLogger(StockTradingApp.class);

  public StockTradingApp() {
    super("StockTradingApp");
  }

  @Override
  protected Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // serdes
    Serde<String> stringSerde = Serdes.String();
    Serde<StockTransaction> stockTransactionSerde = JsonSerde.of(StockTransaction.class);
    Serde<ShareVolume> shareVolumeSerde = JsonSerde.of(ShareVolume.class);
    Serde<TopNList> topNListSerde = JsonSerde.of(TopNList.class);

    // state stores
    String volumeStoreName = "ShareVolumeStore";
    KeyValueBytesStoreSupplier volumeStoreSupplier = Stores.inMemoryKeyValueStore(volumeStoreName);

    String topNStoreName = "TopNStore";
    KeyValueBytesStoreSupplier topNStoreSupplier = Stores.inMemoryKeyValueStore(topNStoreName);

    // create KTable of accumulated share volumes
    builder.stream(
        "transactions",
        Consumed.with(stringSerde, stockTransactionSerde)
            .withName("SourceStockTransactions"))
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
        .peek((industry, topN) -> logger.info("Top N List for industry {}: {}", industry, topN), Named.as("LogResult"))
        // write to output topic
        .to("topn-list", Produced.with(stringSerde, stringSerde).withName("TopNListSink"));

    return builder.build();
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
