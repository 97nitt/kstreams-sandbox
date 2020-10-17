package sandbox.kafka.streams.zmart;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class CorrelatedPurchase {

  private String customerId;
  private List<String> itemsPurchased;
  private double totalAmount;
  private Date firstPurchaseTime;
  private Date secondPurchaseTime;

  private CorrelatedPurchase(Builder builder) {
    this.customerId = builder.customerId;
    this.itemsPurchased = builder.itemsPurchased;
    this.totalAmount = builder.totalAmount;
    this.firstPurchaseTime = builder.firstPurchaseTime;
    this.secondPurchaseTime = builder.secondPurchaseTime;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Purchase one, Purchase two) {
    Builder builder = new Builder();
    if (one != null) {
      builder.customerId(one.getCustomerId());
      builder.addItemPurchased(one.getItemPurchased());
      builder.addAmount(one.getPrice() * (double) one.getQuantity());
      builder.firstPurchaseTime(one.getPurchaseTime());
    }
    if (two != null) {
      builder.customerId(two.getCustomerId());
      builder.addItemPurchased(two.getItemPurchased());
      builder.addAmount(two.getPrice() * (double) two.getQuantity());
      builder.secondPurchaseTime(two.getPurchaseTime());
    }
    return builder;
  }

  public static final class Builder {

    private String customerId;
    private List<String> itemsPurchased;
    private double totalAmount;
    private Date firstPurchaseTime;
    private Date secondPurchaseTime;

    private Builder() {

    }

    public Builder customerId(String customerId) {
      this.customerId = customerId;
      return this;
    }

    public Builder addItemPurchased(String itemPurchased) {
      if (this.itemsPurchased == null) {
        this.itemsPurchased = new ArrayList<>();
      }
      this.itemsPurchased.add(itemPurchased);
      return this;
    }

    public Builder addAmount(double amount) {
      this.totalAmount += amount;
      return this;
    }

    public Builder firstPurchaseTime(Date firstPurchaseTime) {
      this.firstPurchaseTime = firstPurchaseTime;
      return this;
    }

    public Builder secondPurchaseTime(Date secondPurchaseTime) {
      this.secondPurchaseTime = secondPurchaseTime;
      return this;
    }

    public CorrelatedPurchase build() {
      return new CorrelatedPurchase(this);
    }
  }
}
