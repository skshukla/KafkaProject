package com.sachin.work.kafka.consumer;

import com.google.gson.Gson;
import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class ConsumerTest extends BaseTest {

  @Value("${kafka.topic}")
  private String TOPIC;

  @Autowired
  private KafkaConsumer<String, KafkaMessage<Contact>> kafkaConsumer;

  @Test
  public void testConsumer() {
    try {
      this.kafkaConsumer.subscribe(Arrays.asList(TOPIC));

      while (true) {
        final ConsumerRecords<String, KafkaMessage<Contact>> records = this.kafkaConsumer.poll(1000);
        for (final ConsumerRecord<String, KafkaMessage<Contact>> record : records) {
          final KafkaMessage<Contact> kafkaMessage = record.value();
          System.out.println(String.format("Partition {%s}, Offset {%s}, Key {%s}, Value {%s}", record.partition(), record.offset(), record.key(), new Gson().toJson(kafkaMessage)));
        }
      }
    } catch (final WakeupException e) {
      System.out.println(String.format("Exception {%s}", e.getMessage()));
    } finally {
      kafkaConsumer.close();
    }
  }

}