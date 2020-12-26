package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


class ProducerTest extends BaseTest {

  @Autowired
  private SimpleProducer simpleProducer;

  @Test
  void produceMessage_FireAndForget() throws InterruptedException, ExecutionException {
    this.simpleProducer.produce("Message produced from Java Layer");
  }


  @Test
  void sendMessageToDifferentPartitions() throws InterruptedException, ExecutionException {
    push("1", "Message {1} produced from Java Layer, Sync");
    push("2", "Message {2} produced from Java Layer, Sync");
    push("3", "Message {3} produced from Java Layer, Sync");
    push("4", "Message {4} produced from Java Layer, Sync");
    push("5", "Message {5} produced from Java Layer, Sync");
  }

  private void push(final String key, final String msgVal) throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce(key, msgVal);
    final SendResult<String, String> result = future.get();
    System.out.println(String.format("Message pushed to partition {%d} with offset as {%d}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
  }

  @Test
  void produceMessage_Sync() throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce("Message produced from Java Layer, Sync");
    final SendResult<String, String> result = future.get();
    System.out.println("Metadata : " + result.getRecordMetadata().offset());
    System.out.println("Execution Done for method produceMessage_Sync()");
  }

  @Test
  void produceMessage_ASync() throws InterruptedException, ExecutionException {
    final ListenableFuture<SendResult<String, String>> future =  this.simpleProducer.produce("Message produced from Java Layer, Async");
    future.addCallback( new ListenableFutureCallback<SendResult<String, String>>() {

      @Override
      public void onSuccess(final SendResult<String, String> result) {
        System.out.println(String.format("Sent the message successfully with offset {%d} .", result.getRecordMetadata().offset()));
      }

      @Override
      public void onFailure(final Throwable ex) {
        System.out.println(String.format("Exception while sending message {%s}", ex.getMessage()));
      }
    });

    System.out.println("Execution Done for method produceMessage_ASync()");
    Thread.currentThread().sleep(1000);
  }
}
