package org.apache.samza.sql.test;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.samza.sql.examples.HrSchemaExample;
import org.junit.Test;


/**
 * Created by xiliu on 5/10/17.
 */
public class TestSchemaJson {


  @Test
  public void testTableFactory() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("EMPLOYEES", new HrSchemaExample.EmployeeTableFactory().create(rootSchema, "EMPLOYEES", Collections.emptyMap(), null));
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
            "select * from EMPLOYEES");
    Util.print(resultSet);

    resultSet.close();
    statement.close();
    connection.close();
  }
}
