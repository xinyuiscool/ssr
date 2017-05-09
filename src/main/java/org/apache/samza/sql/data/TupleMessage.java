package org.apache.samza.sql.data;

public class TupleMessage {

  private final Object[] value;

  public TupleMessage(Object[] value) {
    this.value = value;
  }

  public Object[] getValue() {
    return value;
  }
}
