package com.sachin.work.kafka.streams;


import com.sachin.work.kafka.BaseTest;
import com.sachin.work.kafka.producer.SimpleProducer;
import com.sachin.work.kafka.util.GenUtil;
import com.sachin.work.kafka.vo.KafkaMessage;
import com.sachin.work.kafka.vo.KafkaMessageDeserializer;
import com.sachin.work.kafka.vo.KafkaMessageSerializer;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;


public class KStreamTest extends BaseTest {

  private final String APP_ID = "my-app";
  private final int START_STREAM_FOR_MINUTES = 5;
  private final int TIME_DIFF_IN_EACH_MESSAGE_PRODUCED_IN_MS = 2 * 1000;

  @Value("${kstream.simple.user.topic}")
  private String TOPIC_USER;

  @Value("${kstream.simple.dep-vice-count.topic}")
  private String TOPIC_DEP_VICE_COUNT;

  @Value("${kstream.simple.total-user-count.topic}")
  private String TOPIC_TOTAL_USER_COUNT;


  private static final String STORE_NAME = "user-count-info-store";

  private static final boolean START_PRODUCER = Boolean.TRUE;
  private static final boolean START_KSTREAM = Boolean.TRUE;

  @Autowired
  private SimpleProducer simpleProducer;

  @Test
  public void testKStream() throws Exception {

    // https://spring.io/blog/2019/12/06/stream-processing-with-spring-cloud-stream-and-apache-kafka-streams-part-5-application-customizations

    final ExecutorService manager = Executors.newFixedThreadPool(2);
    manager.submit(new MyCallable());
    manager.shutdown();


    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    final StreamsConfig streamingConfig = new StreamsConfig(props);
    final Serde<String> stringSerde = Serdes.String();
    final Serde<KafkaMessage<UserInfoVO>> kafkaMessageSerde =  Serdes.serdeFrom(new KafkaMessageSerializer<UserInfoVO>(), new KafkaMessageDeserializer<UserInfoVO>());
    final Serde<KafkaMessage<UserCountInDeptVO>> kafkaMessageSerde2 =  Serdes.serdeFrom(new KafkaMessageSerializer<UserCountInDeptVO>(), new KafkaMessageDeserializer<UserCountInDeptVO>());
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, KafkaMessage<UserInfoVO>> simpleFirstStream = builder.stream(TOPIC_USER, Consumed
        .with(stringSerde, kafkaMessageSerde));

    StoreBuilder<KeyValueStore<Integer, Integer>> countStoreSupplier =
        Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(STORE_NAME),
            Serdes.Integer(),
            Serdes.Integer());
    KeyValueStore<Integer, Integer> countStore = countStoreSupplier.build();
    builder.addStateStore(countStoreSupplier);


    final KStream<String, KafkaMessage<UserCountInDeptVO>> simpleSecondStream = simpleFirstStream.transformValues(
        new ValueTransformerSupplier<KafkaMessage<UserInfoVO>, KafkaMessage<UserCountInDeptVO>>() {

          @Override
          public ValueTransformer<KafkaMessage<UserInfoVO>, KafkaMessage<UserCountInDeptVO>> get() {
            return new UserCountInDeptVOTransformer();
          }
        }, STORE_NAME);

    simpleSecondStream.to( TOPIC_DEP_VICE_COUNT, Produced.with(stringSerde, kafkaMessageSerde2));
    final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamingConfig);

    if (START_KSTREAM) {
      kafkaStreams.start();
    }

    manager.awaitTermination(START_STREAM_FOR_MINUTES, TimeUnit.MINUTES);

    if (START_KSTREAM) {
      kafkaStreams.close();
    }

  }

  class MyCallable implements Callable<Void> {

    // Random user ids from 11 to 110
    private final int user_id_st = 11;
    private final int user_id_end = 111;

    // Random dep ids from 11 to 110
    private final int dep_id_st = 0;
    private final int dep_id_end = 10;


    private final Map<Integer, Integer> USER_DEPT_MAPPING = new ConcurrentHashMap<Integer, Integer>();

    @Override
    public Void call() throws Exception {

      long begin = System.currentTimeMillis();
      while (System.currentTimeMillis() < begin + START_STREAM_FOR_MINUTES * 60 * 1000) {


        if (START_PRODUCER) {
          final int userId = GenUtil.getRandomNumBetween(user_id_st, user_id_end);
          final int deptId = Objects.isNull(USER_DEPT_MAPPING.get(userId))? GenUtil.getRandomNumBetween(dep_id_st, dep_id_end) : USER_DEPT_MAPPING.get(userId); //To ensure if once department is assigned, it belongs to there always.
          simpleProducer.produce(TOPIC_USER, String.valueOf(userId), new KafkaMessage<UserInfoVO>(new UserInfoVO(
              userId, GenUtil.getRandomName(4), deptId)));
        } else {
          System.out.println("Skipping producing the messages!!");
        }

        Thread.currentThread().sleep(TIME_DIFF_IN_EACH_MESSAGE_PRODUCED_IN_MS);
      }
      return null;
    }
  }

  class UserInfoVO {
    private int userId;
    private String randomString;
    private int departmentId;

    public UserInfoVO(){

    }

    public UserInfoVO(final int userId, final String randomString, final int departmentId){
      this.userId = userId;
      this.randomString = randomString;
      this.departmentId = departmentId;
    }

    public String toString() {
      return String.format("UserInfoVO : userId {%s}, randomString {%s}, departmentId {%s}", userId,
          randomString, departmentId);
    }

    public int getUserId() {
      return userId;
    }

    public void setUserId(int userId) {
      this.userId = userId;
    }

    public String getRandomString() {
      return randomString;
    }

    public void setRandomString(String randomString) {
      this.randomString = randomString;
    }

    public int getDepartmentId() {
      return departmentId;
    }

    public void setDepartmentId(int departmentId) {
      this.departmentId = departmentId;
    }
  }

  class UserCountInDeptVO {

    private int deptId;
    private int userCount;

    public int getDeptId() {
      return deptId;
    }

    public void setDeptId(int deptId) {
      this.deptId = deptId;
    }

    public int getUserCount() {
      return userCount;
    }

    public void setUserCount(int userCount) {
      this.userCount = userCount;
    }
  }

  class UserCountInDeptVOTransformer implements ValueTransformer<KafkaMessage<UserInfoVO>, KafkaMessage<UserCountInDeptVO>> {

    private KeyValueStore<Integer, Integer> stateStore;
    private final String storeName = STORE_NAME;
    private ProcessorContext processorContext;

    @Override
    public void init(final ProcessorContext processorContext) {
      this.processorContext = processorContext;
      System.out.println("Going to get the store.");
      this.stateStore = (KeyValueStore) this.processorContext.getStateStore(storeName);
      System.out.println("Got the store...." + this.processorContext.getStateStore(storeName));
    }

    @Override
    public KafkaMessage<UserCountInDeptVO> transform(final KafkaMessage<UserInfoVO> userInfoVO) {
      final UserCountInDeptVO vo = new UserCountInDeptVO();
      final Integer depId = ((Double)((java.util.Map<String, Object>)userInfoVO.getData()).get("departmentId")).intValue();
      final int countSoFar = Objects.isNull(this.stateStore.get(depId)) ? 0 : this.stateStore.get(depId).intValue();
      System.out.println(String.format("depId {%d}, countSoFar {%d}", depId, countSoFar));
      this.stateStore.put(depId, countSoFar+1);
      vo.setDeptId(depId);
      vo.setUserCount(countSoFar+1);
      return new KafkaMessage<>(vo);
    }

    @Override
    public void close() {

    }
  }


}
