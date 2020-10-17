package sandbox.kafka.streams.zmart;

import java.util.Date;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class Purchase {

  private final String customerId;
  private final String firstName;
  private final String lastName;
  private final String creditCardNumber;
  private final String department;
  private final String employeeId;
  private final String itemPurchased;
  private final int quantity;
  private final double price;
  private final Date purchaseTime;
  private final String zipCode;

  private Purchase(Builder builder) {
    this.customerId = builder.customerId;
    this.firstName = builder.firstName;
    this.lastName = builder.lastName;
    this.creditCardNumber = builder.creditCardNumber;
    this.department = builder.department;
    this.employeeId = builder.employeeId;
    this.itemPurchased = builder.itemPurchased;
    this.quantity = builder.quantity;
    this.price = builder.price;
    this.purchaseTime = builder.purchaseTime;
    this.zipCode = builder.zipCode;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Purchase purchase) {
    Builder builder = new Builder();
    builder.customerId = purchase.customerId;
    builder.firstName = purchase.firstName;
    builder.lastName = purchase.lastName;
    builder.creditCardNumber = purchase.creditCardNumber;
    builder.department = purchase.department;
    builder.employeeId = purchase.employeeId;
    builder.itemPurchased = purchase.itemPurchased;
    builder.quantity = purchase.quantity;
    builder.price = purchase.price;
    builder.purchaseTime = purchase.purchaseTime;
    builder.zipCode = purchase.zipCode;
    return builder;
  }

  public static final class Builder {

    private String customerId;
    private String firstName;
    private String lastName;
    private String creditCardNumber;
    private String department;
    private String employeeId;
    private String itemPurchased;
    private int quantity;
    private double price;
    private Date purchaseTime;
    private String zipCode;

    private Builder() {
    }

    public Builder customerId(String customerId) {
      this.customerId = customerId;
      return this;
    }

    public Builder firstName(String firstName) {
      this.firstName = firstName;
      return this;
    }

    public Builder lastName(String lastName) {
      this.lastName = lastName;
      return this;
    }

    public Builder creditCardNumber(String creditCardNumber) {
      this.creditCardNumber = creditCardNumber;
      return this;
    }

    public Builder department(String department) {
      this.department = department;
      return this;
    }

    public Builder employeeId(String employeeId) {
      this.employeeId = employeeId;
      return this;
    }

    public Builder itemPurchased(String itemPurchased) {
      this.itemPurchased = itemPurchased;
      return this;
    }

    public Builder quantity(int quantity) {
      this.quantity = quantity;
      return this;
    }

    public Builder price(double price) {
      this.price = price;
      return this;
    }

    public Builder purchaseTime(Date purchaseTime) {
      this.purchaseTime = purchaseTime;
      return this;
    }

    public Builder zipCode(String zipCode) {
      this.zipCode = zipCode;
      return this;
    }

    public Builder maskCreditCard() {
      Objects.requireNonNull(this.creditCardNumber, "Credit Card can't be null");
      String last4Digits = this.creditCardNumber.split("-")[3];
      this.creditCardNumber = "xxxx-xxxx-xxxx-" + last4Digits;
      return this;
    }

    public Purchase build() {
      return new Purchase(this);
    }
  }
}
