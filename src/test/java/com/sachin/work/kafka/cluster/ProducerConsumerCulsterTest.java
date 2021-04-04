package com.sachin.work.kafka.cluster;

import com.google.gson.Gson;
import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.producer.SimpleProducer;
import com.sachin.work.kafka.util.GenUtil;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

public class ProducerConsumerCulsterTest extends BaseTest {

  private final String USER_DATA_FILE = "src/test/java/com/sachin/work/kafka/cluster/dummy_stream_user_data.csv";

  private final int N_PRODUCER_THREADS = 2;

  @Value("${cluster.topic}")
  private String CLUSTER_TOPIC;

  @Autowired
  private SimpleProducer simpleProducer;

  @Autowired
  @Qualifier("kafkaConsumer")
  private KafkaConsumer<String, String> kafkaConsumer;


  /**
   * mvn -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runProducerThreads test
   *
   * For given number of Producer Threads {@link #N_PRODUCER_THREADS N_PRODUCER_THREADS}, it would produce the messages to topic
   *
   *
   */

  @Test
  public void runProducerThreads() throws Exception {
    final List<String> fileContents = GenUtil.getJsonFromCSVFile(new File(USER_DATA_FILE));

    final MyProducer runnableProducer = new MyProducer(fileContents, this.simpleProducer);

    for (int i = 0; i < N_PRODUCER_THREADS; i++) {
      final Thread t = new Thread(runnableProducer);
      t.start();
    }

    Thread.currentThread().sleep(5 * 60 * 1000);

  }


  /**
   *  mvn -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runConsumer test
   */

  @Test
  public void runConsumer() {

    try {
      this.kafkaConsumer.subscribe(Arrays.asList(CLUSTER_TOPIC));
      int i=0;
      while (true) {
        GenUtil.println(String.format("Inside Consumer while loop for i = {%d}", ++i));
        final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(1 * 1000);
        if (records.count() > 0) {
          GenUtil.println(String.format("Got the Consumer records for i = {%d}, total records got {%d}\n", i, records.count()));
        }
        for (final ConsumerRecord<String, String> record : records) {
          final String kafkaMessage = record.value();
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

  class MyProducer implements Runnable {

    final List<String> fileContents;
    final SimpleProducer simpleProducer;
    int counter = 0;


    public MyProducer(final List<String> fileContents, final SimpleProducer simpleProducer) {
      this.fileContents = fileContents;
      this.simpleProducer = simpleProducer;
    }

    @Override
    public void run() {

      while (this.counter < this.fileContents.size()) {
        synchronized (fileContents) {
          while ((Thread.currentThread().getId() % N_PRODUCER_THREADS) != (counter
              % N_PRODUCER_THREADS)) {
            try {
              fileContents.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          ListenableFuture<SendResult<String, String>> result = simpleProducer.produce(CLUSTER_TOPIC, null, fileContents.get(counter));
          try {
            GenUtil.println(String
                .format("For counter {%d} Produced the message for Topic {%s}, Partition {%s}, Offset {%s} : message {%s}", counter,
                    result.get().getRecordMetadata().topic(), result.get().getRecordMetadata().partition(), result.get().getRecordMetadata().offset(), fileContents.get(counter)));
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ExecutionException e) {
            e.printStackTrace();
          }

          ++counter;
          this.fileContents.notifyAll();
        }
        try {
          Thread.currentThread().sleep(1 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        System.out.println();
      }

    }
  }

}
