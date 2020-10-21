package sandbox.kafka.streams.trading;

import java.util.Date;
import lombok.Data;

@Data
public class StockTransaction {

  private String symbol;
  private String industry;
  private Date timestamp;
  private int shares;
  private double price;
}
