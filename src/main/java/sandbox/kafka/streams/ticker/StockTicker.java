package sandbox.kafka.streams.ticker;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Repartitioned;
import sandbox.kafka.streams.KafkaStreamsApplication;
import sandbox.kafka.streams.serde.JsonSerde;
import sandbox.kafka.streams.util.KafkaAdmin;

public class StockTicker extends KafkaStreamsApplication {

  public StockTicker() {
    super("StockTicker");
  }

  @Override
  protected Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // create serializers/deserializers
    Serde<String> stringSerde = Serdes.String();
    Serde<StockQuote> stockQuoteSerde = JsonSerde.of(StockQuote.class);

    // create stream of stock quotes
    KStream<String, StockQuote> quotes = builder.stream(
        "quotes",
        Consumed.with(stringSerde, stockQuoteSerde).withName("SourceQuotes"))
        .selectKey((key, quote) -> quote.getSymbol(), Named.as("KeyByStockSymbol"))
        .repartition(Repartitioned.with(stringSerde, stockQuoteSerde).withName("QuotesByStockSymbol"));

    quotes.print(Printed.<String, StockQuote>toSysOut().withLabel("quotes"));

    // create table of latest stock quotes from stream
    KTable<String, StockQuote> latestQuotes = quotes.toTable(
        Named.as("QuotesToTable"));

    latestQuotes.toStream(Named.as("LatestQuotesToStream")).
        print(Printed.<String, StockQuote>toSysOut().withLabel("latest-quotes"));

    return builder.build();
  }

  public static void main(String... args) {
    // create input topic
    KafkaAdmin admin = new KafkaAdmin("localhost:9092");
    admin.createTopic("quotes", 1, 1);
    admin.close();

    // start app
    StockTicker app = new StockTicker();
    app.run();
  }
}
