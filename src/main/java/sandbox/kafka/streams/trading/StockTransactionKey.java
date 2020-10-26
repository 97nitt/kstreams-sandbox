package sandbox.kafka.streams.trading;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class StockTransactionKey {

  private final String customerId;
  private final String symbol;

  private StockTransactionKey(Builder builder) {
    this.customerId = builder.customerId;
    this.symbol = builder.symbol;
  }

  public static Builder builder(StockTransaction transaction) {
    return new Builder(transaction);
  }

  public static class Builder {

    private String customerId;
    private String symbol;

    private Builder() {
    }

    public Builder(StockTransaction transaction) {
      this.customerId = transaction.getCustomerId();
      this.symbol = transaction.getSymbol();
    }

    public StockTransactionKey build() {
      return new StockTransactionKey(this);
    }
  }
}
