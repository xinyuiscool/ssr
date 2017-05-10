package org.apache.samza.sql.test;

import java.sql.ResultSet;


/**
 * Created by xiliu on 5/10/17.
 */
public class Util {
  public static void print(ResultSet resultSet) throws Exception {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
      }
      System.out.println(buf.toString());
      buf.setLength(0);
    }
  }

}
