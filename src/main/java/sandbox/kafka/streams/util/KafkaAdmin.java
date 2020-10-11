package sandbox.kafka.streams.util;

import java.util.Collections;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaAdmin {

  private final AdminClient client;

  public KafkaAdmin(String brokers) {
    this(AdminClient.create(Collections.singletonMap("bootstrap.servers", brokers)));
  }

  public KafkaAdmin(AdminClient client) {
    this.client = client;
  }

  public void createTopic(String name, int partitions, int replicationFactor) {
    NewTopic topic = new NewTopic(name, partitions, (short) replicationFactor);
    client.createTopics(Collections.singleton(topic));
  }

  public void close() {
    if (client != null) {
      client.close();
    }
  }
}
