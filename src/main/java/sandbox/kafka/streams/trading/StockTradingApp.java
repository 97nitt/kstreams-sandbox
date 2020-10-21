package sandbox.kafka.streams.trading;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;
import sandbox.kafka.streams.util.KafkaAdmin;

public class StockTradingApp extends KafkaStreamsApplication {

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

    // state stores
    String volumeStoreName = "ShareVolumeStore";
    KeyValueBytesStoreSupplier volumeStoreSupplier = Stores.inMemoryKeyValueStore(volumeStoreName);

    // create KTable of accumulated share volumes
    KTable<String, ShareVolume> shareVolumes = builder.stream(
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
                .withValueSerde(shareVolumeSerde));

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
