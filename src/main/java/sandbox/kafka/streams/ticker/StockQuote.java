package sandbox.kafka.streams.ticker;

import java.util.Date;
import lombok.Data;

@Data
public class StockQuote {

  private String symbol;
  private Date timestamp;
  private double price;
}
