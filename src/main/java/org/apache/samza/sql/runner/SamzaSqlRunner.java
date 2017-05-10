package org.apache.samza.sql.runner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.sql.translator.QueryTranslator;
import org.apache.samza.standalone.StandaloneJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaSqlRunner {
  private static final Logger log = LoggerFactory.getLogger(SamzaSqlRunner.class);
  private static final Map<String, String> DEFAULT_SAMZA_CONFIG;

  static {
    final Map<String, String> config = new HashMap<>();
    config.put(JobConfig.JOB_NAME(), "sql-job");
    config.put(JobConfig.PROCESSOR_ID(), "1");
    config.put("job-coordinator.factory", StandaloneJobCoordinatorFactory.class.getName());
    config.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
    DEFAULT_SAMZA_CONFIG = Collections.unmodifiableMap(config);
  }

  public void run(final String sql) {
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
      final Map<String, String> config = new HashMap<>(DEFAULT_SAMZA_CONFIG);
      final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(config));

      final StreamApplication app = (streamGraph, cfg) -> {
        QueryTranslator.translate(sql, streamGraph);
      };
      runner.run(app);

      runner.waitForFinish();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
