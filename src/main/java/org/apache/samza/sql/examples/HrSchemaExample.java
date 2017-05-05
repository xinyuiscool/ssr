package org.apache.samza.sql.examples;

import org.apache.samza.sql.runner.SamzaSqlRunner;


public class HrSchemaExample {
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
      emps = new Employee[]{new Employee(0, 1), new Employee(1, 3), new Employee(2, 10), new Employee(3, 1),
          new Employee(4, 10)};
      depts = new Department[]{new Department(1), new Department(3), new Department(10)};
    }
  }

  public static void main(String[] args) {
    String sql = "select empid from hr.emps where deptno > 1";
    SamzaSqlRunner runner = new SamzaSqlRunner();
    runner.run(sql);
  }
}
