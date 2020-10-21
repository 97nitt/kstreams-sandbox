package sandbox.kafka.streams.trading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class TopNList<T> {

  private final List<T> list;
  private final Comparator<T> comparator;
  private final int n;

  public TopNList(Comparator<T> comparator, int n) {
    this.list = new ArrayList<>();
    this.comparator = comparator;
    this.n = n;
  }

  public TopNList<T> add(T item) {
    list.add(item);
    Collections.sort(list, comparator);
    if (list.size() > n) {
      list.remove(list.size() - 1);
    }
    return this;
  }

  public TopNList<T> remove(T item) {
    this.list.remove(item);
    return this;
  }

  public Iterator<T> iterator() {
    return list.iterator();
  }

  @Override
  public String toString() {
    return list.toString();
  }
}
