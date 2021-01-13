package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.generatedVo.Employee;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class SimpleProducer<T> {

  @Autowired
  @Qualifier("kafkaTemplate_Simple")
  private KafkaTemplate<String, String> kafkaTemplate_Simple;

  @Autowired
  @Qualifier("kafkaTemplate_GenericMessage")
  private KafkaTemplate<String, KafkaMessage<T>> kafkaTemplate_GenericMessage;

  public ListenableFuture<SendResult<String, String>> produce(final String topic, final String key, final String msgVal) {
    final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msgVal);
    return this.kafkaTemplate_Simple.send(record);
  }

  public ListenableFuture<SendResult<String, KafkaMessage<T>>> produce(final String topic, final String key, final KafkaMessage<T> data) {
    final ProducerRecord<String, KafkaMessage<T>> record = new ProducerRecord<String, KafkaMessage<T>>(topic, key, data);
    return this.kafkaTemplate_GenericMessage.send(record);
  }

}
