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
  private final int purchasePoints;
  private final int totalPoints;

  private Rewards(Builder builder) {
    this.customerId = builder.customerId;
    this.purchaseTotal = builder.purchaseTotal;
    this.purchasePoints = builder.purchasePoints;
    this.totalPoints = builder.totalPoints;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Purchase purchase, Integer currentPoints) {
    Builder builder = new Builder();
    builder.customerId = purchase.getCustomerId();
    builder.purchaseTotal = purchase.getPrice() * (double) purchase.getQuantity();
    builder.purchasePoints = (int) builder.purchaseTotal;
    builder.totalPoints = builder.purchasePoints;
    if (currentPoints != null) {
      builder.totalPoints += currentPoints;
    }
    return builder;
  }

  public static final class Builder {

    private String customerId;
    private double purchaseTotal;
    private int purchasePoints;
    private int totalPoints;

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

    public Builder purchasePoints(int purchasePoints) {
      this.purchasePoints = purchasePoints;
      return this;
    }

    public Builder totalPoints(int totalPoints) {
      this.totalPoints = totalPoints;
      return this;
    }

    public Rewards build() {
      return new Rewards(this);
    }
  }
}
