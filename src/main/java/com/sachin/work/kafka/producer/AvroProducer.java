package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.vo.Employee;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class AvroProducer {


  @Value("${kafka.topic}")
  private String TOPIC;

  @Autowired
  @Qualifier("kafkaTemplate_AvroSerializerAndDeserializer")
  private KafkaTemplate<String, Employee> kafkaTemplate_AvroSerializerAndDeserializer;

  public ListenableFuture<SendResult<String, Employee>> produce(final String key, final Employee employee) {
    final ProducerRecord<String, Employee> record = new ProducerRecord<>(TOPIC,key , employee);
    return this.kafkaTemplate_AvroSerializerAndDeserializer.send(record);
  }

}
