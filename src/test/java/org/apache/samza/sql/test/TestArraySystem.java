package org.apache.samza.sql.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.examples.HrSchemaExample;
import org.apache.samza.sql.system.ArraySystemFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;
import org.apache.samza.sql.util.Base64Serializer;

import static org.junit.Assert.assertEquals;


/**
 * Created by xiliu on 5/11/17.
 */
public class TestArraySystem {

  @Test
  public void testConsumer() throws Exception {
    String empsStr = Base64Serializer.serialize(new HrSchemaExample.HrSchema().emps);
    Map<String, String> configs = new HashMap<>();
    configs.put("systems.test.source", empsStr);

    Config config = new MapConfig(configs);
    SystemStreamPartition ssp = new SystemStreamPartition("test", "stream", new Partition(0));
    SystemFactory factory = new ArraySystemFactory();
    SystemConsumer consumer = factory.getConsumer("test", config, null);
    consumer.register(ssp, null);
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopes = consumer.poll(Collections.singleton(ssp), 1000);
    assertEquals(envelopes.get(ssp).size(), 5);
  }


}
