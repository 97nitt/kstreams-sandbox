package sandbox.kafka.streams.zmart;

import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class PurchasePattern {

  private final String zipCode;
  private final String item;
  private final Date date;
  private final double amount;

  private PurchasePattern(Builder builder) {
    this.zipCode = builder.zipCode;
    this.item = builder.item;
    this.date = builder.date;
    this.amount = builder.amount;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Purchase purchase) {
    Builder builder = new Builder();
    builder.zipCode = purchase.getZipCode();
    builder.item = purchase.getItemPurchased();
    builder.date = purchase.getPurchaseDate();
    builder.amount = purchase.getPrice();
    return builder;
  }

  public static final class Builder {

    private String zipCode;
    private String item;
    private Date date;
    private double amount;

    private Builder() {
    }

    public Builder zipCode(String zipCode) {
      this.zipCode = zipCode;
      return this;
    }

    public Builder item(String item) {
      this.item = item;
      return this;
    }

    public Builder date(Date date) {
      this.date = date;
      return this;
    }

    public Builder amount(double amount) {
      this.amount = amount;
      return this;
    }

    public PurchasePattern build() {
      return new PurchasePattern(this);
    }
  }
}
