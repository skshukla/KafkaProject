package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


class ProducerTest extends BaseTest {

  @Autowired
  private SimpleProducer simpleProducer;

  @Test
  void testProduceMessage() {
    this.simpleProducer.produce("ABCD2");
  }
}
