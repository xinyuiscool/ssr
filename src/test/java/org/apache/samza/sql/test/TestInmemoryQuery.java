package org.apache.samza.sql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.samza.sql.examples.HrSchemaExample;


public class TestInmemoryQuery {

    public static void main(String [] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("hr", new ReflectiveSchema(new HrSchemaExample.HrSchema()));
        Statement statement = calciteConnection.createStatement();
        //ResultSet resultSet = statement.executeQuery(
        //        "select * from \"hr\".\"depts\"");
        ResultSet resultSet = statement.executeQuery(
                "select d.\"deptno\", min(e.\"empid\")\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "join \"hr\".\"depts\" as d\n"
                        + "  on e.\"deptno\" = d.\"deptno\"\n"
                        + "group by d.\"deptno\"\n"
                        + "having count(*) > 1");
        Util.print(resultSet);

        resultSet.close();
        statement.close();
        connection.close();
    }

}
