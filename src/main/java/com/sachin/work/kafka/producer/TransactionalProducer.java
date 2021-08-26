package com.sachin.work.kafka.producer;

import com.sachin.work.kafka.util.GenUtil;
import com.sachin.work.kafka.vo.KafkaMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

@Service
public class TransactionalProducer<T> {

    @Autowired
    @Qualifier("kafkaTemplate_Transactional")
    private KafkaTemplate<String, String> kafkaTemplate_Transactional;


    private void printRecordMetadata(RecordMetadata recordMetadata) {
        GenUtil.println(String.format("Record meta info is : Topic {%s}, Partition {%d}, Offset {%d}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
    }

    public void produce(final String topic, final List<String> msgList, final int waitTimeInMillis) {

        GenUtil.println(String.format("The kafka template is a transactional template {%s}, msgList size {%d}", this.kafkaTemplate_Transactional.isTransactional(), msgList.size()));

        this.kafkaTemplate_Transactional.executeInTransaction(new KafkaOperations.OperationsCallback<String, String, Void>() {

            @Override
            public Void doInOperations(final KafkaOperations<String, String> kafkaOperations) {

                IntStream.range(0, msgList.size()).forEach(i -> {
                    GenUtil.println(String.format("[%d] of [%d] : Going to push message {%s} to topic {%s}", i, msgList.size(), msgList.get(i), topic));
                    try {
                        final SendResult<String, String> result = kafkaOperations.send(new ProducerRecord<>(topic, null, msgList.get(i))).get();
                        printRecordMetadata(result.getRecordMetadata());
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        throw new RuntimeException(ex);
                    } catch (ExecutionException ex) {
                        ex.printStackTrace();
                        throw new RuntimeException(ex);
                    }
                    GenUtil.sleep(waitTimeInMillis);
                });
                return null;
            }
        });

        GenUtil.println("Execution done !!");
    }
}
