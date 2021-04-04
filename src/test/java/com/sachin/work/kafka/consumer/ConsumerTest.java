package com.sachin.work.kafka.consumer;

import com.google.gson.Gson;
import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.util.GenUtil;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import java.util.Arrays;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
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


  /**
   * mvn -Dtest=com.sachin.work.kafka.consumer.ConsumerTest#testConsumer test
   */

  @Test
  public void testConsumer() {
    try {
      this.kafkaConsumer.subscribe(Arrays.asList(TOPIC));
      int i=0;
      while (true) {
        System.out.println(String.format("[%s] - Inside Consumer while loop for i = {%d}",
            GenUtil.getDateToStr(new Date()), ++i));
        final ConsumerRecords<String, KafkaMessage<Contact>> records = this.kafkaConsumer.poll(1 * 1000);
        if (records.count() > 0) {
          System.out.println(String.format("[%s] - Got the Consumer recors for i = {%d}, total records got {%d}\n",
              GenUtil.getDateToStr(new Date()), i, records.count()));
        }
        for (final ConsumerRecord<String, KafkaMessage<Contact>> record : records) {
          final KafkaMessage<Contact> kafkaMessage = record.value();
          System.out.println(String.format("Partition {%s}, Offset {%s}, Key {%s}, Value {%s}", record.partition(), record.offset(), record.key(), new Gson().toJson(kafkaMessage)));
        }
      }
    } catch (final WakeupException e) {
      System.out.println(String.format("Exception {%s}", e.getMessage()));
      e.printStackTrace();
    } finally {
      kafkaConsumer.close();
    }
  }

  private void me() {
    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          System.out.println("Going to call wakeup ");
          kafkaConsumer.wakeup();
          System.out.println("Called wakeup successfully");
        } catch (Exception ex) {
          ex.printStackTrace();
        }

      }
    }, 5 * 1000);
  }

}