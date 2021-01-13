package com.sachin.work.kafka.vo;

import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;


public class KafkaMessageSerializer<T> implements Serializer<KafkaMessage<T>> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String s, KafkaMessage<T> tKafkaMessage) {
    return new Gson().toJson(tKafkaMessage).getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public byte[] serialize(String topic, Headers headers, KafkaMessage<T> data) {
    return new Gson().toJson(data).getBytes(Charset.forName("UTF-8"));
  }


  @Override
  public void close() {

  }
}
