package com.sachin.work.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class SimpleProducer {


  @Value("${kafka.topic}")
  private String TOPIC;

  @Autowired
  @Qualifier("kafkaTemplate")
  private KafkaTemplate<String, String> kafkaTemplate;

  public ListenableFuture<SendResult<String, String>> produce(final String msg) {
    return this.produce(null, msg);
  }

  public ListenableFuture<SendResult<String, String>> produce(final String key, final String msgVal) {
    return this.produce(TOPIC, key, msgVal);
  }

  public ListenableFuture<SendResult<String, String>> produce(final String topic, final String key, final String msgVal) {
    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msgVal);
    return this.kafkaTemplate.send(record);
  }

}
