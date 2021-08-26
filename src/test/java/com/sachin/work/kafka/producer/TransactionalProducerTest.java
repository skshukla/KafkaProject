package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.util.GenUtil;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 *
 */

class TransactionalProducerTest extends BaseTest {


  private String TRANSACTIONAL_TOPIC = "txnal-t-001";

  @Autowired
  private  TransactionalProducer transactionalProducer;

  @Test
  void produceMessage_me() throws InterruptedException, ExecutionException {
    int N = 20;
    final List<String> msgListOne = new ArrayList<>();
    final List<String> msgListTwo = new ArrayList<>();
    IntStream.range(0, N).forEach(i -> {
      msgListOne.add(GenUtil.getRandomName(4));
      msgListTwo.add(GenUtil.getRandomName(20));
    });

    ExecutorService manager = Executors.newFixedThreadPool(2);
    manager.submit(() -> {
      this.transactionalProducer.produce(TRANSACTIONAL_TOPIC, msgListOne, 1500);
    });

    manager.submit(() -> {
      this.transactionalProducer.produce(TRANSACTIONAL_TOPIC, msgListTwo, 1500);
    });

    Thread.currentThread().sleep(60 * 1000);
  }

}
