package org.apache.samza.sql.test;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestInmemoryQuery {

    public static class Employee {
        public final int empid;
        public final int deptno;
        public Employee(int empid, int deptno) {
            this.empid = empid;
            this.deptno = deptno;
        }
    }

    public static class Department {
        public final int deptno;
        public Department(int deptno) {
            this.deptno = deptno;
        }
    }

    public static class HrSchema {
        public final Employee[] emps;
        public final Department[] depts;

        public HrSchema() {
            emps = new Employee[] {
                new Employee(0, 1),
                new Employee(1,3),
                new Employee(2,10),
                new Employee(3,1),
                new Employee(4,10)
            };

            depts = new Department[] {
                new Department(1),
                new Department(3),
                new Department(10)
            };
        }
    }

    public static void main(String [] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
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
        print(resultSet);

        resultSet.close();
        statement.close();
        connection.close();
    }

    static void print(ResultSet resultSet) throws Exception {
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
