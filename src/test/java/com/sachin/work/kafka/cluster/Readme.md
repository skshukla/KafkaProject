git clone https://github.com/skshukla/KafkaProject.git
cd KafkaProject

Open One Terminal for Producer and issue command

```java
    mvn clean -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runProducerThreads test
```

Open Three Terminal for Consumers and issue command (In each terminal)

```java
    mvn  -Dtest=com.sachin.work.kafka.cluster.ProducerConsumerCulsterTest#runConsumer test
```  

