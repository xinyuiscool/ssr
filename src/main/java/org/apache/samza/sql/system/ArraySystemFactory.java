package org.apache.samza.sql.system;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;


public class ArraySystemFactory implements SystemFactory {

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry metricsRegistry) {
    return new ArraySystemConsumer(config);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry metricsRegistry) {
    // no producer
    return null;
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SimpleSystemAdmin();
  }
}
