package org.apache.samza.sql.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.impl.AbstractTableQueryable;


/**
 * Created by xiliu on 5/10/17.
 */
public abstract class ReflectiveTableFactory implements TableFactory<Table> {
  private final Class clazz;
  private final Object[] array;

  public ReflectiveTableFactory(Class clazz, Object[] array) {
    this.clazz = clazz;
    this.array = array;
  }

  @Override
  public final Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    return new AbstractQueryableTable(clazz) {

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return ((JavaTypeFactory) typeFactory).createType(clazz);
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
          public Enumerator<T> enumerator() {
            @SuppressWarnings("unchecked")
            final List<T> list = (List) Arrays.asList(array);
            return Linq4j.enumerator(list);
          }
        };
      }
    };
  }
}
