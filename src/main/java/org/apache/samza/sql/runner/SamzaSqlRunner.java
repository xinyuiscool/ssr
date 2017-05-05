package org.apache.samza.sql.runner;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.samza.sql.examples.HrSchemaExample;
import org.apache.samza.sql.planner.QueryPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaSqlRunner {
  private static final Logger log = LoggerFactory.getLogger(SamzaSqlRunner.class);

  public void run(String sql) {
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      // TODO: use real schema instead of hardcoding
      rootSchema.add("hr", new ReflectiveSchema(new HrSchemaExample.HrSchema()));

      QueryPlanner planner = new QueryPlanner(rootSchema);
      planner.getPlan(sql);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
