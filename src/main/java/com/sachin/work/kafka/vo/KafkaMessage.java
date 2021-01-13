package com.sachin.work.kafka.vo;

import java.util.UUID;

public class KafkaMessage<T> {

  private String uuid = UUID.randomUUID().toString();
  private T data;
  private String dataType;

  public KafkaMessage(final T data) {
    this.data = data;
    this.dataType = this.data.getClass().getName();
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }

  public String getUuid() {
    return uuid;
  }

  public String getDataType() {
    return dataType;
  }
}
