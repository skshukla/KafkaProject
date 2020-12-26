package com.sachin.work.kafka.producer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class CustomPartitioner implements Partitioner {

  @Override
  public int partition(final String topic, final Object key, byte[] keyBytes, Object value, byte[] valueBytes, final Cluster cluster) {

    final List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    System.out.println(String.format("numPartitions is {%d}", numPartitions));

    if (!(key instanceof String)) return 0;
    final String keyStr = (String)key;

    try {
      final int keyVal = Integer.parseInt(keyStr);
      final int p = keyVal%numPartitions;
      System.out.println(String.format("Message with key {%s}, Val {%s} is going to partition {%d}",keyStr, String.valueOf(value), p));
      return p;
    } catch (final Exception ex) {
      return 0;
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(final Map<String, ?> map) {

  }
}