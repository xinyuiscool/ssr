package org.apache.samza.sql.system;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.sql.util.Base64Serializer;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;


public class ArraySystemConsumer implements SystemConsumer {
  private final Config config;

  public ArraySystemConsumer(Config config) {
    this.config = config;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String s)
  {
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> set, long l) throws InterruptedException {
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopeMap = new HashMap<>();
    set.forEach(ssp -> {
      List<IncomingMessageEnvelope> envelopes = Arrays.stream(getArrayObjects(ssp.getSystemStream().getStream(), config))
          .map(object -> new IncomingMessageEnvelope(ssp, null, null, object))
          .collect(Collectors.toList());
      envelopes.add(IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp));
      envelopeMap.put(ssp, envelopes);
    });
    return envelopeMap;
  }

  private static Object[] getArrayObjects(String stream, Config config) {
    try {
      return Base64Serializer.deserialize(config.get("streams." + stream + ".source"), Object[].class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
