package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.generatedVo.Employee;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 *
 */

class SimpleProducerTest extends BaseTest {



  @Value("${kafka.compacted.topic.01}")
  private String COMPACTED_TOPIC_01;


  @Autowired
  private SimpleProducer simpleProducer;

  @Test
  void produceMessage_FireAndForget() throws InterruptedException, ExecutionException {
    this.simpleProducer.produce(TOPIC, null,"This is Sample Message from Java Layer!!");
  }


  /**
   * mvn -Dtest=com.sachin.work.kafka.producer.SimpleProducerTest#produceMessage_FireAndForget_Generic test
   */

  @Test
  void produceMessage_FireAndForget_Generic() throws InterruptedException, ExecutionException {
    final KafkaMessage<Contact> kafkaMessage = new KafkaMessage<>(new Contact(1, "Shukla, Sachin Kumar"));
    this.simpleProducer.produce(TOPIC, kafkaMessage.getUuid(), kafkaMessage);
  }


  @Test
  void sendMessageToDifferentPartitions() throws InterruptedException, ExecutionException {

    for (int i=0; i<10000; i++) {
      push("1", String.format("my message {%d}", (i+1)));
//      Thread.sleep(20*1000);
    }

  }

  private void push(final String key, final String msgVal) throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce(TOPIC, key, msgVal);
    final SendResult<String, String> result = future.get();
    System.out.println(String.format("Message pushed to partition {%d} with offset as {%d}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
  }

  @Test
  void produceMessage_Sync() throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce(TOPIC, null, "Message produced from Java Layer, Sync");
    final SendResult<String, String> result = future.get();
    System.out.println("Metadata : " + result.getRecordMetadata().offset());
    System.out.println("Execution Done for method produceMessage_Sync()");
  }

  @Test
  void produceMessage_ASync() throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce(TOPIC, "k5", "Message produced from Java Layer, Async Retries");
    future.addCallback( new ListenableFutureCallback<SendResult<String, String>>() {

      @Override
      public void onSuccess(final SendResult<String, String> result) {
        System.out.println(String.format("Sent the message successfully with offset {%d} .", result.getRecordMetadata().offset()));
      }

      @Override
      public void onFailure(final Throwable ex) {
        System.out.println(String.format("Exception while sending message {%s}", ex.getMessage()));
        ex.printStackTrace();
      }
    });

    System.out.println("Execution Done for method produceMessage_ASync()");
    Thread.currentThread().sleep(3 * 1000);
  }

}
