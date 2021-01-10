package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.vo.Employee;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class AvroProducerTest extends BaseTest {

  @Value("${kafka.compacted.topic.01}")
  private String COMPACTED_TOPIC_01;

  @Autowired
  private AvroProducer avroProducer;

  @Test
  void produceMessage() throws InterruptedException, ExecutionException {
    final Employee employee = new Employee(1, "Kanpur-4", null);
    final ListenableFuture<SendResult<String, Employee>> future = this.avroProducer.produce(String.valueOf(employee.getName()), employee);
    final SendResult<String, Employee> result = future.get();
    System.out.println("Metadata : " + result.getRecordMetadata().offset());
  }


  @Test
  void produceMessagesToCompactTopic() throws InterruptedException, ExecutionException {
    this.avroProducer.produce(COMPACTED_TOPIC_01, "a1", new Employee(1, "Sachin A1_01", "Singapore"));
    this.avroProducer.produce(COMPACTED_TOPIC_01, "a2", new Employee(2, "Sachin A2_01", "Bangalore"));
    this.avroProducer.produce(COMPACTED_TOPIC_01, "a3", new Employee(3, "Sachin A3_01", "Kanpur"));

    for(int i=0; i<3;i++) {
      this.avroProducer.produce(COMPACTED_TOPIC_01, "k1", new Employee((i+1), String.format("Sachin K1_0%s", (i+1)), "Singapore"));
    }

    for(int i=0; i<3;i++) {
      this.avroProducer.produce(COMPACTED_TOPIC_01, "k2", new Employee((i+1), String.format("Sachin K2_0%s", (i+1)), "Bangalore"));
    }

    for(int i=0; i<3;i++) {
      this.avroProducer.produce(COMPACTED_TOPIC_01, "k3", new Employee((i+1), String.format("Sachin K3_0%s", (i+1)), "Kanpur"));
    }

    Thread.currentThread().sleep(10 * 1000);
    this.avroProducer.produce(COMPACTED_TOPIC_01, "a1", new Employee(4, "Sachin A1_02", "Singapore"));
  }
}
