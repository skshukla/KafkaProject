package com.sachin.work.kafka.vo;

public class Contact {
  private int id;
  private String name;

  public Contact() {

  }

  public Contact(final int id, final String name) {
    this.id = id;
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
