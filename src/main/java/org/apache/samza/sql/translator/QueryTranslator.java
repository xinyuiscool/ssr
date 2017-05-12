package org.apache.samza.sql.translator;

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.TupleMessage;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class QueryTranslator {

  public static void translate(String sql, StreamGraph streamGraph, Config config) {
    final RelRoot relRoot = QueryPlanner.plan(sql, config);
    final TranslatorContext context = new TranslatorContext(streamGraph, relRoot);
    final RelNode node = relRoot.project();

    node.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        new ScanTranslator().translate(scan, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator().translate(project, context);
        return node;
      }
    });

    MessageStreamImpl<TupleMessage> stream = (MessageStreamImpl<TupleMessage>) context.getMessageStream(node.getId());
    stream.sink((TupleMessage m, MessageCollector collector, TaskCoordinator coordinator) -> {
      List<String> values = Arrays.stream(m.getValue()).map(Object::toString).collect(Collectors.toList());
      String output = Joiner.on(',').join(values);
      System.out.println("OUTPUT: " + output);
    });
  }
}
