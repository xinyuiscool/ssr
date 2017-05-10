package org.apache.samza.sql.data;

import java.lang.reflect.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.samza.SamzaException;


public class TupleMessage {

  private final Object[] value;

  public TupleMessage(Object[] value) {
    this.value = value;
  }

  public Object[] getValue() {
    return value;
  }

  public static TupleMessage fromReflection(Object tuple, RelDataType sqlType) {
    if (sqlType.isStruct()) {
      Object[] out = new Object[sqlType.getFieldCount()];

      for (RelDataTypeField field : sqlType.getFieldList()) {
        RelDataType fieldType = field.getType();

        // TODO: Implement support for maps, arrays and sets.
        if (isMap(sqlType) || isCollection(sqlType)) {
          throw new SamzaException(String.format("Unsupported SQL type %s", sqlType.toString()));
        }

        try {
          Field reflectedField = tuple.getClass().getDeclaredField(field.getName());
          out[field.getIndex()] = reflectedField.get(tuple);
        } catch (Exception e) {
          throw new SamzaException(e);
        }
      }

      return new TupleMessage(out);

    } else {
      throw new SamzaException(String.format("Unsupported SQL type %s", sqlType.toString()));
    }
  }

  private static boolean isMap(RelDataType type) {
    return type.getKeyType() != null && type.getValueType() != null;
  }

  private static boolean isCollection(RelDataType type) {
    return type.getComponentType() != null;
  }
}
