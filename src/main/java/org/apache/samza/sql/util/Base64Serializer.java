package org.apache.samza.sql.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;


public class Base64Serializer {
  private Base64Serializer() {}

  public static String serializeUnchecked(Serializable serializable) {
    try {
      return serialize(serializable);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serialize(Serializable serializable) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(serializable);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  public static <T> T deserializeUnchecked(String serialized, Class<T> klass) {
    try {
      return deserialize(serialized, klass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserialize(String serialized, Class<T> klass) throws IOException, ClassNotFoundException {
    final byte[] bytes = Base64.getDecoder().decode(serialized);
    final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
    @SuppressWarnings("unchecked")
    T object = (T)ois.readObject();
    ois.close();
    return object;
  }
}
