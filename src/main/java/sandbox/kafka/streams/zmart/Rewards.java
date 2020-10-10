package sandbox.kafka.streams.zmart;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class Rewards {

  private final String customerId;
  private final double purchaseTotal;
  private final int rewardPoints;

  private Rewards(Builder builder) {
    this.customerId = builder.customerId;
    this.purchaseTotal = builder.purchaseTotal;
    this.rewardPoints = builder.rewardPoints;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Purchase purchase) {
    Builder builder = new Builder();
    builder.customerId = purchase.getCustomerId();
    builder.purchaseTotal = purchase.getPrice() * (double) purchase.getQuantity();
    builder.rewardPoints = (int) builder.purchaseTotal;
    return builder;
  }

  public static final class Builder {

    private String customerId;
    private double purchaseTotal;
    private int rewardPoints;

    private Builder() {
    }

    public Builder customerId(String customerId) {
      this.customerId = customerId;
      return this;
    }

    public Builder purchaseTotal(double purchaseTotal) {
      this.purchaseTotal = purchaseTotal;
      return this;
    }

    public Builder rewardPoints(int rewardPoints) {
      this.rewardPoints = rewardPoints;
      return this;
    }

    public Rewards build() {
      return new Rewards(this);
    }
  }
}
