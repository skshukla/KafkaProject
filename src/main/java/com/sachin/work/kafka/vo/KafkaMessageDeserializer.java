package com.sachin.work.kafka.vo;

import com.google.gson.Gson;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaMessageDeserializer<T> implements Deserializer<KafkaMessage<T>> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public KafkaMessage<T> deserialize(String s, byte[] bytes) {
    final KafkaMessage kafkaMessage = new Gson().fromJson(new String(bytes), KafkaMessage.class);
    return kafkaMessage;
  }

  @Override
  public KafkaMessage<T> deserialize(String topic, Headers headers, byte[] data) {
    final KafkaMessage kafkaMessage = new Gson().fromJson(new String(data), KafkaMessage.class);
    return kafkaMessage;
  }

  @Override
  public void close() {

  }
}
