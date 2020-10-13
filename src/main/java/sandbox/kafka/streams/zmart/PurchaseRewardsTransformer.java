package sandbox.kafka.streams.zmart;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PurchaseRewardsTransformer implements ValueTransformer<Purchase, Rewards> {

  private final String storeName;

  // state store of accumulated rewards points, keyed by customer id
  private KeyValueStore<String, Integer> store;

  public PurchaseRewardsTransformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.store = (KeyValueStore<String, Integer>) context.getStateStore(storeName);
  }

  @Override
  public Rewards transform(Purchase purchase) {
    // get current points from state store
    Integer points = store.get(purchase.getCustomerId());

    // build rewards data
    Rewards rewards = Rewards.builder(purchase, points).build();

    // update state store
    store.put(purchase.getCustomerId(), rewards.getTotalPoints());

    // return rewards data
    return rewards;
  }

  @Override
  public void close() {

  }
}
