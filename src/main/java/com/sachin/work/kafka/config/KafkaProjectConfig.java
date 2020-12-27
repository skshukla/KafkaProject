package com.sachin.work.kafka.config;

import com.google.common.collect.ImmutableMap;
import com.sachin.work.kafka.vo.Employee;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
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
  public KafkaTemplate<String, String> kafkaTemplate() {
    final KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(this.producerFactory());
    return kafkaTemplate;
  }

  @Bean
  public KafkaTemplate<String, Employee> kafkaTemplate_AvroSerializerAndDeserializer() {
    final KafkaTemplate<String, Employee> kafkaTemplate = new KafkaTemplate<>(this.producerFactory_AvroSerializerAndDeserializer());
    return kafkaTemplate;
  }


  private ProducerFactory<String, String> producerFactory() {
    final Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

//    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

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
