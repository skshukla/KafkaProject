package com.sachin.work.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

@SpringBootTest
public class BaseTest {

  @Autowired
  protected ApplicationContext applicationContext;

  @Test
  void contextSuccessfulLoad() {
    Assert.notNull(applicationContext, "Application Context cannot be null");
  }

}
