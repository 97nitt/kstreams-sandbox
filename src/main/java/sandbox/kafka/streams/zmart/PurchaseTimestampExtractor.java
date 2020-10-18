package sandbox.kafka.streams.zmart;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class PurchaseTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    Purchase purchase = (Purchase) record.value();
    return purchase.getPurchaseTime().getTime();
  }
}
