package org.apache.samza.sql.data;

public interface Expression {
  Object execute(Object[] inputValues);
  void execute(Object[] inputValues, Object[] results);
}
