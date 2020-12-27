package com.sachin.work.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

@SpringBootTest
public class BaseTest {

  @BeforeEach
  public void setUp() {
    System.out.println("Inside BaseTest#setUp().....");
  }


  @AfterEach
  public void tearDown() {
    System.out.println("Inside BaseTest#tearDown()....., Sleep for sometime to give the method time to finish its stuff");

    try {
      Thread.currentThread().sleep(2 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Autowired
  protected ApplicationContext applicationContext;

  @Test
  public void contextSuccessfulLoad() {
    Assert.notNull(applicationContext, "Application Context cannot be null");
  }

}
