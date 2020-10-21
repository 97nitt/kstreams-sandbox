package sandbox.kafka.streams.trading;

import lombok.Data;

@Data
public class ShareVolume {

  private final String symbol;
  private final int shares;

  private ShareVolume(Builder builder) {
    this.symbol = builder.symbol;
    this.shares = builder.shares;
  }

  public static Builder builder(StockTransaction transaction) {
    Builder builder = new Builder();
    builder.symbol = transaction.getSymbol();
    builder.shares = transaction.getShares();
    return builder;
  }

  public static Builder builder(ShareVolume volume1, ShareVolume volume2) {
    Builder builder = new Builder();
    if (volume1 != null) {
      builder.symbol = volume1.symbol;
      builder.shares += volume1.shares;
    }
    if (volume2 != null) {
      builder.symbol = volume2.symbol;
      builder.shares += volume2.shares;
    }
    return builder;
  }

  public static class Builder {

    private String symbol;
    private int shares;

    private Builder() {

    }

    public ShareVolume build() {
      return new ShareVolume(this);
    }
  }
}
