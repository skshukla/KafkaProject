package com.sachin.work.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class SimpleProducer {


  @Value("${kafka.topic}")
  private String TOPIC;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  public void produce(final String msg) {
    final ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", null, msg);
    this.kafkaTemplate.send(record);
  }

}
