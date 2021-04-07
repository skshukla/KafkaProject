package com.sachin.work.kafka.cluster;

import com.google.gson.Gson;
import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.producer.SimpleProducer;
import com.sachin.work.kafka.util.GenUtil;
import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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

  private final int TIME_INTERVAL_BETWEEN_PRODUCER_EVENTS_IN_MS = 500;

  private final int PROGRAM_MAX_EXECUTION_TIME_IN_MINUTES = 15;


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
    List<Map<?, ?>> dataMapList = GenUtil.readObjectsFromCsv(new File(USER_DATA_FILE));

//    final List<String> fileContents = GenUtil.getJsonFromCSVFile(new File(USER_DATA_FILE));

    final MyProducer runnableProducer = new MyProducer(dataMapList, this.simpleProducer);

    for (int i = 0; i < N_PRODUCER_THREADS; i++) {
      final Thread t = new Thread(runnableProducer);
      t.start();
    }

    Thread.currentThread().sleep(PROGRAM_MAX_EXECUTION_TIME_IN_MINUTES * 60 * 1000);

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
        GenUtil.println(String.format("Inside Consumer (subscribe) while loop for i = {%d}", ++i));
        final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(5 * 1000);
        if (records.count() <= 0) {
          continue;
        }
        GenUtil.println(String.format("Got the Consumer records for i = {%d}, total records got {%d}\n", i, records.count()));
        for (final ConsumerRecord<String, String> record : records) {
          final String kafkaMessage = record.value();
          GenUtil.println(String.format("Partition {%s}, Offset {%s}, Key {%s}, Value {%s}", record.partition(), record.offset(), record.key(), new Gson().toJson(kafkaMessage)));
        }
      }
    } catch (final WakeupException e) {
      GenUtil.printlnErr(String.format("Exception {%s}", e.getMessage()));
      e.printStackTrace();
    } finally {
      kafkaConsumer.close();
    }

  }

  /**
   * Run the method {@link #runProducerThreads runProducerThreads()} and then execute this consumer.
   *  mvn -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runConsumerForSpecificTopicPartition test
   */

  @Test
  public void runConsumerForSpecificTopicPartition() {

    try {
      this.kafkaConsumer.assign(Arrays.asList(new TopicPartition(CLUSTER_TOPIC, 1)));
      int i=0;
      while (true) {
        GenUtil.println(String.format("Inside Consumer (assign) while loop for i = {%d}", ++i));
        final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(1 * 1000);
        if (records.count() <= 0) {
          continue;
        }
        GenUtil.println(String.format("Got the Consumer records for i = {%d}, total records got {%d}\n", i, records.count()));
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

    final List<Map<?, ?>> dataMapList;
    final SimpleProducer simpleProducer;
    int counter = 0;


    public MyProducer(final List<Map<?, ?>> dataMapList, final SimpleProducer simpleProducer) {
      this.dataMapList = dataMapList;
      this.simpleProducer = simpleProducer;
    }

    @Override
    public void run() {

      while (this.counter < this.dataMapList.size()) {
        synchronized (dataMapList) {
          while ((Thread.currentThread().getId() % N_PRODUCER_THREADS) != (counter
              % N_PRODUCER_THREADS)) {
            try {
              dataMapList.wait();
            } catch (final InterruptedException e) {
              e.printStackTrace();
            }
          }
          final Map<String, Object> dataMap = (Map<String, Object>)dataMapList.get(counter);
          dataMap.put("event_time", GenUtil.getDateToStr(new Date()));
          final String eventStr = GenUtil.writeAsJson(dataMap);
          GenUtil.writeAsJson(dataMapList.get(counter));
          final ListenableFuture<SendResult<String, String>> result = simpleProducer.produce(CLUSTER_TOPIC, null, eventStr);
          try {
            GenUtil.println(String
                .format("For counter {%d} Produced the message for Topic {%s}, Partition {%s}, Offset {%s} : message {%s}", counter,
                    result.get().getRecordMetadata().topic(), result.get().getRecordMetadata().partition(), result.get().getRecordMetadata().offset(), eventStr));
            Thread.currentThread().sleep(TIME_INTERVAL_BETWEEN_PRODUCER_EVENTS_IN_MS);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          } catch (final ExecutionException e) {
            e.printStackTrace();
          }

          ++counter;
          this.dataMapList.notifyAll();
        }
        System.out.println();
      }

    }
  }

}
