package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.vo.Employee;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class AvroProducerTest extends BaseTest {

  @Autowired
  private AvroProducer avroProducer;

  @Test
  void produceMessage() throws InterruptedException, ExecutionException {
    final Employee employee = new Employee(1, "Kanpur-4", null);
    final ListenableFuture<SendResult<String, Employee>> future = this.avroProducer.produce(String.valueOf(employee.getName()), employee);
    final SendResult<String, Employee> result = future.get();
    System.out.println("Metadata : " + result.getRecordMetadata().offset());
  }
}
