package org.apache.samza.sql.examples;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlRunner;
import org.apache.samza.sql.system.ArraySystemFactory;
import org.apache.samza.sql.test.ReflectiveTableFactory;
import org.apache.samza.sql.util.Base64Serializer;


public class HrSchemaExample {
  public static class Employee implements Serializable {
    public final int empid;
    public final int deptno;
    public Employee(int empid, int deptno) {
      this.empid = empid;
      this.deptno = deptno;
    }
  }

  public static class Department implements Serializable{
    public final int deptno;
    public Department(int deptno) {
      this.deptno = deptno;
    }
  }

  public static class HrSchema {
    public final Employee[]   emps;
    public final Department[] depts;

    public HrSchema() {
      emps = new Employee[]{new Employee(0, 1), new Employee(1, 3), new Employee(2, 10), new Employee(3, 1),
          new Employee(4, 10)};
      depts = new Department[]{new Department(1), new Department(3), new Department(10)};
    }
  }

  public static final class EmployeeTableFactory extends ReflectiveTableFactory {
    public EmployeeTableFactory() {
      super(HrSchemaExample.Employee.class, new HrSchema().emps);
    }
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.hr.samza.factory", ArraySystemFactory.class.getName());
    configs.put("streams.EMPLOYEES.samza.system", "hr");
    configs.put("streams.EMPLOYEES.source", Base64Serializer.serialize(new HrSchema().emps));
    configs.put("streams.EMPLOYEES.schema", EmployeeTableFactory.class.getName());

    //String sql = "select empid from hr.emps where deptno > 1";
    //String sql = "select * from EMPLOYEES";
    String sql = "select empid from EMPLOYEES";
    SamzaSqlRunner runner = new SamzaSqlRunner(new MapConfig(configs));
    runner.run(sql);
  }
}
