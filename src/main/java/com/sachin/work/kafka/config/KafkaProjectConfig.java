package com.sachin.work.kafka.config;

import com.google.common.collect.ImmutableMap;
import com.sachin.work.kafka.generatedVo.Employee;
import com.sachin.work.kafka.vo.Contact;
import com.sachin.work.kafka.vo.KafkaMessage;
import com.sachin.work.kafka.vo.KafkaMessageDeserializer;
import com.sachin.work.kafka.vo.KafkaMessageSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProjectConfig {

  @Value("${kafka.brokers}")
  private String KAFKA_BROKERS;

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate_Simple() {
    final KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(this.producerFactory_Simple());
    return kafkaTemplate;
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate_Transactional() {
    final KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(this.producerFactory_Transactional());
    return kafkaTemplate;
  }

  @Bean
  public <T> KafkaTemplate<String, KafkaMessage<T>> kafkaTemplate_GenericMessage() {
    final KafkaTemplate<String, KafkaMessage<T>> kafkaTemplate = new KafkaTemplate<>(this.producerFactory_GenericMessage());
    return kafkaTemplate;
  }


  @Bean
  public KafkaConsumer<String, String> kafkaConsumer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ga_0");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(props);
  }


  @Bean
  public KafkaConsumer<String, KafkaMessage<Contact>> kafkaConsumer_Generic() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ga_1");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaMessageDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return new KafkaConsumer<>(props);
  }

  @Bean
  public KafkaTemplate<String, Employee> kafkaTemplate_AvroSerializerAndDeserializer() {
    final KafkaTemplate<String, Employee> kafkaTemplate = new KafkaTemplate<>(this.producerFactory_AvroSerializerAndDeserializer());
    return kafkaTemplate;
  }


  private ProducerFactory<String, String> producerFactory_Simple() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

//    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
//    props.put(ProducerConfig.RETRIES_CONFIG, 3);
//    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

    return new DefaultKafkaProducerFactory<>(props);
  }


  private ProducerFactory<String, String> producerFactory_Transactional() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
    return new DefaultKafkaProducerFactory<>(props);
  }

  private <T> ProducerFactory<String, KafkaMessage<T>> producerFactory_GenericMessage() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMessageSerializer.class);
    return new DefaultKafkaProducerFactory<>(props);
  }


  private ProducerFactory<String, Employee> producerFactory_AvroSerializerAndDeserializer() {
    final Map<String, Object> props = ImmutableMap
        .of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS
            , ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class
            , ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class
            , "schema.registry.url", "http://localhost:8081"
        );
    return new DefaultKafkaProducerFactory<>(props);
  }
}
